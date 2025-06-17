use crate::*;
use anchor_client::solana_sdk::pubkey::Pubkey;
use core::result::Result::Ok;
use solana_sdk::{account::Account, clock::Clock};
use std::{collections::HashMap, ops::Deref};

#[derive(Debug)]
pub struct SwapExactInQuote {
    pub amount_out: u64,
    pub fee: u64,
}

#[derive(Debug)]
pub struct SwapExactOutQuote {
    pub amount_in: u64,
    pub fee: u64,
}

fn validate_swap_activation(
    lb_pair: &LbPair,
    current_timestamp: u64,
    current_slot: u64,
) -> Result<()> {
    ensure!(
        lb_pair.status()?.eq(&PairStatus::Enabled),
        "Pair is disabled"
    );

    let pair_type = lb_pair.pair_type()?;
    if pair_type.eq(&PairType::Permission) {
        let activation_type = lb_pair.activation_type()?;
        let current_point = match activation_type.deref() {
            ActivationType::Slot => current_slot,
            ActivationType::Timestamp => current_timestamp,
        };

        ensure!(
            current_point >= lb_pair.activation_point,
            "Pair is disabled"
        );
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub fn quote_exact_out(
    lb_pair_pubkey: Pubkey,
    lb_pair: &LbPair,
    mut amount_out: u64,
    swap_for_y: bool,
    bin_arrays: HashMap<Pubkey, BinArray>,
    bitmap_extension: Option<&BinArrayBitmapExtension>,
    clock: &Clock,
    mint_x_account: &Account,
    mint_y_account: &Account,
) -> Result<SwapExactOutQuote> {
    let current_timestamp = clock.unix_timestamp as u64;
    let current_slot = clock.slot;
    let epoch = clock.epoch;

    validate_swap_activation(lb_pair, current_timestamp, current_slot)?;

    let mut lb_pair = *lb_pair;
    lb_pair.update_references(current_timestamp as i64)?;

    let mut total_amount_in: u64 = 0;
    let mut total_fee: u64 = 0;

    let (in_mint_account, out_mint_account) = if swap_for_y {
        (mint_x_account, mint_y_account)
    } else {
        (mint_y_account, mint_x_account)
    };

    amount_out =
        calculate_transfer_fee_included_amount(out_mint_account, amount_out, epoch)?.amount;

    while amount_out > 0 {
        let active_bin_array_pubkey = get_bin_array_pubkeys_for_swap(
            lb_pair_pubkey,
            &lb_pair,
            bitmap_extension,
            swap_for_y,
            1,
        )?
        .pop()
        .context("Pool out of liquidity")?;

        let mut active_bin_array = bin_arrays
            .get(&active_bin_array_pubkey)
            .cloned()
            .context("Active bin array not found")?;

        loop {
            if !active_bin_array.is_bin_id_within_range(lb_pair.active_id)? || amount_out == 0 {
                break;
            }

            lb_pair.update_volatility_accumulator()?;

            let active_bin = active_bin_array.get_bin_mut(lb_pair.active_id)?;
            let price = active_bin.get_or_store_bin_price(lb_pair.active_id, lb_pair.bin_step)?;

            if !active_bin.is_empty(!swap_for_y) {
                let bin_max_amount_out = active_bin.get_max_amount_out(swap_for_y);
                if amount_out >= bin_max_amount_out {
                    let max_amount_in = active_bin.get_max_amount_in(price, swap_for_y)?;
                    let max_fee = lb_pair.compute_fee(max_amount_in)?;

                    total_amount_in = total_amount_in
                        .checked_add(max_amount_in)
                        .context("MathOverflow")?;

                    total_fee = total_fee.checked_add(max_fee).context("MathOverflow")?;

                    amount_out = amount_out
                        .checked_sub(bin_max_amount_out)
                        .context("MathOverflow")?;
                } else {
                    let amount_in = Bin::get_amount_in(amount_out, price, swap_for_y)?;
                    let fee = lb_pair.compute_fee(amount_in)?;

                    total_amount_in = total_amount_in
                        .checked_add(amount_in)
                        .context("MathOverflow")?;

                    total_fee = total_fee.checked_add(fee).context("MathOverflow")?;

                    amount_out = 0;
                }
            }

            if amount_out > 0 {
                lb_pair.advance_active_bin(swap_for_y)?;
            }
        }
    }

    total_amount_in = total_amount_in
        .checked_add(total_fee)
        .context("MathOverflow")?;

    total_amount_in =
        calculate_transfer_fee_included_amount(in_mint_account, total_amount_in, epoch)?.amount;

    Ok(SwapExactOutQuote {
        amount_in: total_amount_in,
        fee: total_fee,
    })
}

#[allow(clippy::too_many_arguments)]
pub fn quote_exact_in(
    lb_pair_pubkey: Pubkey,
    lb_pair: &LbPair,
    amount_in: u64,
    swap_for_y: bool,
    bin_arrays: HashMap<Pubkey, BinArray>,
    bitmap_extension: Option<&BinArrayBitmapExtension>,
    clock: &Clock,
    mint_x_account: &Account,
    mint_y_account: &Account,
) -> Result<SwapExactInQuote> {
    let current_timestamp = clock.unix_timestamp as u64;
    let current_slot = clock.slot;
    let epoch = clock.epoch;

    validate_swap_activation(lb_pair, current_timestamp, current_slot)?;

    let mut lb_pair = *lb_pair;
    lb_pair.update_references(current_timestamp as i64)?;

    let mut total_amount_out: u64 = 0;
    let mut total_fee: u64 = 0;

    let (in_mint_account, out_mint_account) = if swap_for_y {
        (mint_x_account, mint_y_account)
    } else {
        (mint_y_account, mint_x_account)
    };

    let transfer_fee_excluded_amount_in =
        calculate_transfer_fee_excluded_amount(in_mint_account, amount_in, epoch)?.amount;

    let mut amount_left = transfer_fee_excluded_amount_in;

    while amount_left > 0 {
        //找到有流动性的流动性仓位数组（BinArray）的地址（Pubkey）
        let active_bin_array_pubkey = get_bin_array_pubkeys_for_swap(
            lb_pair_pubkey,
            &lb_pair,
            bitmap_extension,
            swap_for_y,
            1,
        )?
        .pop()
        .context("Pool out of liquidity")?;

        //拿到 BinArray 的地址后，代码会从传入的 bin_arrays 这个 HashMap 中取出对应的 BinArray 数据。
        //这个 HashMap 相当于一个缓存，预先加载了可能用到的所有 BinArray。
        let mut active_bin_array = bin_arrays
            .get(&active_bin_array_pubkey)
            .cloned()
            .context("Active bin array not found")?;

        //这个循环负责在当前找到的 BinArray (大箱子) 内部，逐个 Bin (小格子) 地进行兑换。
        loop {
            //第一个循环退出条件:检查当前池子活跃的 Bin ID (lb_pair.active_id) 是否还在这个 BinArray 的范围内。
            //如果不在，说明这个“大箱子”里的流动性已经用完了，需要 break 掉内层循环，让外层循环去寻找下一个 BinArray。
            //第二个循环退出条件:amount_left == 0: 如果钱已经花完了，也就没必要继续了，直接 break。
            if !active_bin_array.is_bin_id_within_range(lb_pair.active_id)? || amount_left == 0 {
                break;
            }

            lb_pair.update_volatility_accumulator()?;

            //首先，代码从当前的 BinArray（大货柜）中取出当前活跃的那个 Bin（小货架）。
            let active_bin = active_bin_array.get_bin_mut(lb_pair.active_id)?;
            //它计算出这个 Bin 的确切价格 price。在Meteora中，每个 Bin 都代表一个固定的价格区间
            let price = active_bin.get_or_store_bin_price(lb_pair.active_id, lb_pair.bin_step)?;

            //这行代码检查这个“bin”上是否还有你想要的代币库存。如果没有，就跳过这个bin，直接去下一个。
            if !active_bin.is_empty(!swap_for_y) {
                let SwapResult {
                    amount_in_with_fees,
                    amount_out,
                    fee,
                    ..
                } = active_bin.swap(amount_left, price, swap_for_y, &lb_pair, None)?;

                amount_left = amount_left
                    .checked_sub(amount_in_with_fees)
                    .context("MathOverflow")?;

                total_amount_out = total_amount_out
                    .checked_add(amount_out)
                    .context("MathOverflow")?;
                total_fee = total_fee.checked_add(fee).context("MathOverflow")?;
            }

            if amount_left > 0 {
                lb_pair.advance_active_bin(swap_for_y)?;
            }
        }
    }

    let transfer_fee_excluded_amount_out =
        calculate_transfer_fee_excluded_amount(out_mint_account, total_amount_out, epoch)?.amount;

    Ok(SwapExactInQuote {
        amount_out: transfer_fee_excluded_amount_out,
        fee: total_fee,
    })
}

///为一笔即将发生的交易（Swap）找到接下来有流动性的流动性仓位数组（BinArray）的地址（Pubkey）
/// 由于 bitmap 的大小有限，Meteora 设计了一套扩展机制：
/// 内部 bitmap: LbPair 账户自身带有一个大小固定的 bitmap。
/// 扩展 bitmap (bitmap_extension): 如果流动性分布范围很广，超出了内部 bitmap 能表示的范围，就可以启用一个或多个 BinArrayBitmapExtension 账户来存储额外的 bitmap。
/// 这段代码的作用就是智能地在这两种 bitmap 中进行搜索，找到我们需要的 BinArray 地址。
pub fn get_bin_array_pubkeys_for_swap(
    lb_pair_pubkey: Pubkey,
    lb_pair: &LbPair,
    bitmap_extension: Option<&BinArrayBitmapExtension>,
    swap_for_y: bool,
    take_count: u8,
) -> Result<Vec<Pubkey>> {
    //根据当前活跃的 Bin ID (lb_pair.active_id) 计算出它所在的 BinArray 的索引。搜索就从这个索引开始
    let mut start_bin_array_idx = BinArray::bin_id_to_bin_array_index(lb_pair.active_id)?;

    //需要注意的是 这里的index并不是bitmap上这个bin_array的位置,而是BinArray 的索引值
    //用来存放找到的、有流动性的 BinArray 的索引
    let mut bin_array_idx = vec![];
    //increment: 搜索方向。如果是用 X 换 Y (swap_for_y is true)，价格是下降的，所以索引要递减 (-1)；反之则递增 (+1)。
    let increment = if swap_for_y { -1 } else { 1 };

    //循环的目的是找到 take_count 个符合条件的 BinArray 索引
    loop {
        if bin_array_idx.len() == take_count as usize {
            break;
        }

        //它检查当前的 start_bin_array_idx 是否已经超出了 LbPair 内部 bitmap 所能管理的范围。
        if lb_pair.is_overflow_default_bin_array_bitmap(start_bin_array_idx) {
            //超出内部范围，需要在扩展 bitmap 中搜索
            //确认 bitmap_extension 账户存在。如果不存在但又需要搜索它，说明没有更多流动性了，直接退出循环
            let Some(bitmap_extension) = bitmap_extension else {
                break;
            };

            //调用扩展 bitmap 的搜索方法，从 start_bin_array_idx 开始按指定方向 (swap_for_y) 寻找下一个为 1 的位。
            let Ok((next_bin_array_idx, has_liquidity)) = bitmap_extension
                .next_bin_array_index_with_liquidity(swap_for_y, start_bin_array_idx)
            else {
                // Out of search range. No liquidity.
                //如果没搜索到 说明没有流动性了
                break;
            };

            // 如果找到了 (has_liquidity 为 true)，就把返回的 next_bin_array_idx 存入结果列表，并更新下一次搜索的起始点 start_bin_array_idx。
            if has_liquidity {
                bin_array_idx.push(next_bin_array_idx);
                start_bin_array_idx = next_bin_array_idx + increment; //这里实在bin_array_index上进行加减
            } else {
                //如果没找到，说明在这个 bitmap_extension 中搜索完了。返回的 next_bin_array_idx 会是这个 bitmap 的边界，
                //代码将 start_bin_array_idx 更新为这个边界值，以便在下一次循环中可以切换回内部 bitmap 或者另一个 bitmap_extension 继续搜索
                start_bin_array_idx = next_bin_array_idx;
            }
        //在内部 bitmap 范围内搜索
        } else {
            //调用 LbPair 自身的搜索方法，在其内部 bitmap 中寻找
            //逻辑和情况一非常相似：找到就记录并更新下一次的起点；没找到就将 start_bin_array_idx 更新到内部 bitmap 的边界，以便下次循环可以切换到扩展 bitmap。
            let Ok((next_bin_array_idx, has_liquidity)) = lb_pair
                .next_bin_array_index_with_liquidity_internal(swap_for_y, start_bin_array_idx)
            else {
                break;
            };

            if has_liquidity {
                bin_array_idx.push(next_bin_array_idx);
                start_bin_array_idx = next_bin_array_idx + increment;
            //场景
            // 我们要向右搜索（swap_for_y = false），所以 increment = 1。
            // 我们需要找到 3 个有流动性的 BinArray（take_count = 3）。
            // 假设当前活跃的 bin_array_index 是 10。
            // 循环过程
            // 第一次循环:
            // 起始点: start_bin_array_idx = 10。
            // 搜索: next_bin_array_index_with_liquidity_internal 从索引 10 开始向右搜索。
            // 结果: 假设它找到了第一个有流动性的 BinArray 在索引 15。所以 next_bin_array_idx = 15。
            // 记录: 我们将 15 添加到结果列表 bin_array_idx 中。bin_array_idx 现在是 [15]。
            // 更新起始点:
            // 如果我们不更新 start_bin_array_idx，下一次循环还会从 10 开始，又会找到 15，陷入死循环。
            // 如果我们只设置为 start_bin_array_idx = next_bin_array_idx (也就是 15)，下一次循环从 15 开始，还是会找到 15，同样陷入死循环。
            // 正确的做法: 我们需要从刚刚找到的位置的下一个位置开始搜索。所以我们执行 start_bin_array_idx = 15 + 1 = 16。
            // 第二次循环:
            // 起始点: start_bin_array_idx = 16。
            // 搜索: 从索引 16 开始向右搜索。
            // 结果: 假设找到了下一个有流动性的 BinArray 在索引 18。next_bin_array_idx = 18。
            // 记录: bin_array_idx 现在是 [15, 18]。
            // 更新起始点: start_bin_array_idx = 18 + 1 = 19。
            // 第三次循环:
            // 起始点: start_bin_array_idx = 19。
            // 搜索: 从索引 19 开始向右搜索。
            // 结果: 假设找到了下一个在索引 25。next_bin_array_idx = 25。
            // 记录: bin_array_idx 现在是 [15, 18, 25]。
            // 更新起始点: start_bin_array_idx = 25 + 1 = 26。
            } else {
                // Switch to external bitmap
                start_bin_array_idx = next_bin_array_idx;
            }
        }
    }

    //循环结束后，bin_array_idx 里就存放了所有找到的 BinArray 的索引
    let bin_array_pubkeys = bin_array_idx
        .into_iter()
        .map(|idx| derive_bin_array_pda(lb_pair_pubkey, idx.into()).0)
        .collect();

    Ok(bin_array_pubkeys)
}

#[cfg(test)]
mod tests {
    use super::*;
    use anchor_client::solana_sdk::clock::Clock;
    use anchor_client::{
        solana_client::nonblocking::rpc_client::RpcClient, solana_sdk::pubkey::Pubkey, Cluster,
    };
    use std::str::FromStr;

    /// Get on chain clock
    async fn get_clock(rpc_client: RpcClient) -> Result<Clock> {
        let clock_account = rpc_client
            .get_account(&anchor_client::solana_sdk::sysvar::clock::ID)
            .await?;

        let clock_state: Clock = bincode::deserialize(clock_account.data.as_ref())?;

        Ok(clock_state)
    }

    #[tokio::test]
    async fn test_swap_quote_exact_out() {
        // RPC client. No gPA is required.
        let rpc_client = RpcClient::new(Cluster::Mainnet.url().to_string());

        let sol_usdc = Pubkey::from_str("HTvjzsfX3yU6BUodCjZ5vZkUrAxMDTrBs3CJaq43ashR").unwrap();

        let lb_pair_account = rpc_client.get_account(&sol_usdc).await.unwrap();

        let lb_pair = LbPairAccount::deserialize(&lb_pair_account.data).unwrap().0;

        let mut mint_accounts = rpc_client
            .get_multiple_accounts(&[lb_pair.token_x_mint, lb_pair.token_y_mint])
            .await
            .unwrap();

        let mint_x_account = mint_accounts[0].take().unwrap();
        let mint_y_account = mint_accounts[1].take().unwrap();

        // 3 bin arrays to left, and right is enough to cover most of the swap, and stay under 1.4m CU constraint.
        // Get 3 bin arrays to the left from the active bin
        let left_bin_array_pubkeys =
            get_bin_array_pubkeys_for_swap(sol_usdc, &lb_pair, None, true, 3).unwrap();

        // Get 3 bin arrays to the right the from active bin
        let right_bin_array_pubkeys =
            get_bin_array_pubkeys_for_swap(sol_usdc, &lb_pair, None, false, 3).unwrap();

        // Fetch bin arrays
        let bin_array_pubkeys = left_bin_array_pubkeys
            .into_iter()
            .chain(right_bin_array_pubkeys.into_iter())
            .collect::<Vec<Pubkey>>();

        let accounts = rpc_client
            .get_multiple_accounts(&bin_array_pubkeys)
            .await
            .unwrap();

        let bin_arrays = accounts
            .into_iter()
            .zip(bin_array_pubkeys.into_iter())
            .map(|(account, key)| {
                (
                    key,
                    BinArrayAccount::deserialize(&account.unwrap().data)
                        .unwrap()
                        .0,
                )
            })
            .collect::<HashMap<_, _>>();

        let usdc_token_multiplier = 1_000_000.0;
        let sol_token_multiplier = 1_000_000_000.0;

        let out_sol_amount = 1_000_000_000;
        let clock = get_clock(rpc_client).await.unwrap();

        let quote_result = quote_exact_out(
            sol_usdc,
            &lb_pair,
            out_sol_amount,
            false,
            bin_arrays.clone(),
            None,
            &clock,
            &mint_x_account,
            &mint_y_account,
        )
        .unwrap();

        let in_amount = quote_result.amount_in + quote_result.fee;

        println!(
            "{} USDC -> exact 1 SOL",
            in_amount as f64 / usdc_token_multiplier
        );

        let quote_result = quote_exact_in(
            sol_usdc,
            &lb_pair,
            in_amount,
            false,
            bin_arrays.clone(),
            None,
            &clock,
            &mint_x_account,
            &mint_y_account,
        )
        .unwrap();

        println!(
            "{} USDC -> {} SOL",
            in_amount as f64 / usdc_token_multiplier,
            quote_result.amount_out as f64 / sol_token_multiplier
        );

        let out_usdc_amount = 200_000_000;

        let quote_result = quote_exact_out(
            sol_usdc,
            &lb_pair,
            out_usdc_amount,
            true,
            bin_arrays.clone(),
            None,
            &clock,
            &mint_x_account,
            &mint_y_account,
        )
        .unwrap();

        let in_amount = quote_result.amount_in + quote_result.fee;

        println!(
            "{} SOL -> exact 200 USDC",
            in_amount as f64 / sol_token_multiplier
        );

        let quote_result = quote_exact_in(
            sol_usdc,
            &lb_pair,
            in_amount,
            true,
            bin_arrays,
            None,
            &clock,
            &mint_x_account,
            &mint_y_account,
        )
        .unwrap();

        println!(
            "{} SOL -> {} USDC",
            in_amount as f64 / sol_token_multiplier,
            quote_result.amount_out as f64 / usdc_token_multiplier
        );
    }

    #[tokio::test]
    async fn test_swap_quote_exact_in() {
        // RPC client. No gPA is required.
        let rpc_client = RpcClient::new(Cluster::Mainnet.url().to_string());

        let sol_usdc = Pubkey::from_str("HTvjzsfX3yU6BUodCjZ5vZkUrAxMDTrBs3CJaq43ashR").unwrap();

        let lb_pair_account = rpc_client.get_account(&sol_usdc).await.unwrap();

        let lb_pair = LbPairAccount::deserialize(&lb_pair_account.data).unwrap().0;

        let mut mint_accounts = rpc_client
            .get_multiple_accounts(&[lb_pair.token_x_mint, lb_pair.token_y_mint])
            .await
            .unwrap();

        let mint_x_account = mint_accounts[0].take().unwrap();
        let mint_y_account = mint_accounts[1].take().unwrap();

        // 3 bin arrays to left, and right is enough to cover most of the swap, and stay under 1.4m CU constraint.
        // Get 3 bin arrays to the left from the active bin
        let left_bin_array_pubkeys =
            get_bin_array_pubkeys_for_swap(sol_usdc, &lb_pair, None, true, 3).unwrap();

        // Get 3 bin arrays to the right the from active bin
        let right_bin_array_pubkeys =
            get_bin_array_pubkeys_for_swap(sol_usdc, &lb_pair, None, false, 3).unwrap();

        // Fetch bin arrays
        let bin_array_pubkeys = left_bin_array_pubkeys
            .into_iter()
            .chain(right_bin_array_pubkeys.into_iter())
            .collect::<Vec<Pubkey>>();

        let accounts = rpc_client
            .get_multiple_accounts(&bin_array_pubkeys)
            .await
            .unwrap();

        let bin_arrays = accounts
            .into_iter()
            .zip(bin_array_pubkeys.into_iter())
            .map(|(account, key)| {
                (
                    key,
                    BinArrayAccount::deserialize(&account.unwrap().data)
                        .unwrap()
                        .0,
                )
            })
            .collect::<HashMap<_, _>>();

        // 1 SOL -> USDC
        let in_sol_amount = 1_000_000_000;

        let clock = get_clock(rpc_client).await.unwrap();

        let quote_result = quote_exact_in(
            sol_usdc,
            &lb_pair,
            in_sol_amount,
            true,
            bin_arrays.clone(),
            None,
            &clock,
            &mint_x_account,
            &mint_y_account,
        )
        .unwrap();

        println!(
            "1 SOL -> {:?} USDC",
            quote_result.amount_out as f64 / 1_000_000.0
        );

        // 100 USDC -> SOL
        let in_usdc_amount = 100_000_000;

        let quote_result = quote_exact_in(
            sol_usdc,
            &lb_pair,
            in_usdc_amount,
            false,
            bin_arrays.clone(),
            None,
            &clock,
            &mint_x_account,
            &mint_y_account,
        )
        .unwrap();

        println!(
            "100 USDC -> {:?} SOL",
            quote_result.amount_out as f64 / 1_000_000_000.0
        );
    }
}
