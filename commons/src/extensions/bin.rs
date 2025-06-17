use crate::*;

pub trait BinExtension {
    fn get_or_store_bin_price(&mut self, id: i32, bin_step: u16) -> Result<u128>;
    fn is_empty(&self, is_x: bool) -> bool;
    fn get_max_amount_out(&self, swap_for_y: bool) -> u64;
    fn get_max_amount_in(&self, price: u128, swap_for_y: bool) -> Result<u64>;
    fn calculate_out_amount(&self, liquidity_share: u128) -> Result<(u64, u64)>;

    fn swap(
        &mut self,
        amount_in: u64,
        price: u128,
        swap_for_y: bool,
        lb_pair: &LbPair,
        host_fee_bps: Option<u16>,
    ) -> Result<SwapResult>;

    fn get_amount_out(amount_in: u64, price: u128, swap_for_y: bool) -> Result<u64>;
    fn get_amount_in(amount_out: u64, price: u128, swap_for_y: bool) -> Result<u64>;
}

impl BinExtension for Bin {
    fn get_or_store_bin_price(&mut self, id: i32, bin_step: u16) -> Result<u128> {
        if self.price == 0 {
            self.price = get_price_from_id(id, bin_step)?;
        }

        Ok(self.price)
    }

    fn is_empty(&self, is_x: bool) -> bool {
        if is_x {
            self.amount_x == 0
        } else {
            self.amount_y == 0
        }
    }

    fn get_max_amount_out(&self, swap_for_y: bool) -> u64 {
        if swap_for_y {
            self.amount_y
        } else {
            self.amount_x
        }
    }

    fn get_max_amount_in(&self, price: u128, swap_for_y: bool) -> Result<u64> {
        if swap_for_y {
            safe_shl_div_cast(self.amount_y.into(), price, SCALE_OFFSET, Rounding::Up)
        } else {
            safe_mul_shr_cast(self.amount_x.into(), price, SCALE_OFFSET, Rounding::Up)
        }
    }

    fn get_amount_in(amount_out: u64, price: u128, swap_for_y: bool) -> Result<u64> {
        if swap_for_y {
            safe_shl_div_cast(amount_out.into(), price, SCALE_OFFSET, Rounding::Up)
        } else {
            safe_mul_shr_cast(amount_out.into(), price, SCALE_OFFSET, Rounding::Up)
        }
    }

    // price = (amount_Y / amount_X) * 2^64
    fn get_amount_out(amount_in: u64, price: u128, swap_for_y: bool) -> Result<u64> {
        if swap_for_y {
            safe_mul_shr_cast(price, amount_in.into(), SCALE_OFFSET, Rounding::Down)
        } else {
            safe_shl_div_cast(amount_in.into(), price, SCALE_OFFSET, Rounding::Down)
        }
    }

    fn calculate_out_amount(&self, liquidity_share: u128) -> Result<(u64, u64)> {
        let out_amount_x = safe_mul_div_cast(
            liquidity_share,
            self.amount_x.into(),
            self.liquidity_supply,
            Rounding::Down,
        )?;

        let out_amount_y = safe_mul_div_cast(
            liquidity_share,
            self.amount_y.into(),
            self.liquidity_supply,
            Rounding::Down,
        )?;
        Ok((out_amount_x, out_amount_y))
    }

    //接收一笔输入金额 amount_in，计算出在这个窗口能兑换到多少输出金额 amount_out，需要支付多少手续费 fee，并最终更新窗口的库存。
    fn swap(
        &mut self,
        amount_in: u64,
        price: u128,
        swap_for_y: bool,
        lb_pair: &LbPair,
        host_fee_bps: Option<u16>,
    ) -> Result<SwapResult> {
        //函数会检查这个窗口最多有多少外币可以给你。这就是这个 Bin 的最大兑换能力。
        let max_amount_out = self.get_max_amount_out(swap_for_y);
        //它计算要把这些外币全部换走，理论上需要多少输入货币
        let mut max_amount_in = self.get_max_amount_in(price, swap_for_y)?;

        //它计算换走全部外币需要支付的最高手续费。
        let max_fee = lb_pair.compute_fee(max_amount_in)?;
        //将理论输入和最高手续费相加，得到掏空这个窗口所需的总成本 max_amount_in。
        max_amount_in = max_amount_in.checked_add(max_fee).context("overflow")?;

        //情况一：你的钱太多，窗口不够换。你带来的钱 (amount_in) 比掏空这个窗口所需的总成本还多
        //在这种情况下，交易会被窗口的库存限制。
        // amount_in_with_fees: 你实际支付的金额就是掏空窗口的成本 max_amount_in。
        //支付的手续费就是最高手续费 max_fee。
        // 你多余的钱会留着去下一个窗口
        let (amount_in_with_fees, amount_out, fee, protocol_fee) = if amount_in > max_amount_in {
            (
                max_amount_in,
                max_amount_out,
                max_fee,
                lb_pair.compute_protocol_fee(max_fee)?,
            )
        } else {
            //情况二：你的钱没那么多，窗口够换。你带来的钱少于或等于掏空窗口的成本。
            //根据你的总输入金额 amount_in，计算出应付的手续费。
            let fee = lb_pair.compute_fee_from_amount(amount_in)?;
            //从你的总输入中减去手续费，得到真正用于兑换的净额。
            let amount_in_after_fee = amount_in.checked_sub(fee).context("overflow")?;
            //用这个净额，按当前窗口的固定汇率 price，计算出你能得到多少外币 amount_out。
            let amount_out = Bin::get_amount_out(amount_in_after_fee, price, swap_for_y)?;
            (
                amount_in,
                std::cmp::min(amount_out, max_amount_out),
                fee,
                lb_pair.compute_protocol_fee(fee)?,
            )
        };

        let host_fee = match host_fee_bps {
            Some(bps) => protocol_fee
                .checked_mul(bps.into())
                .context("overflow")?
                .checked_div(BASIS_POINT_MAX as u64)
                .context("overflow")?,
            None => 0,
        };

        let protocol_fee_after_host_fee = protocol_fee.checked_sub(host_fee).context("overflow")?;

        let amount_into_bin = amount_in_with_fees.checked_sub(fee).context("overflow")?;

        if swap_for_y {
            self.amount_x = self
                .amount_x
                .checked_add(amount_into_bin)
                .context("overflow")?;
            self.amount_y = self.amount_y.checked_sub(amount_out).context("overflow")?;
        } else {
            self.amount_y = self
                .amount_y
                .checked_add(amount_into_bin)
                .context("overflow")?;
            self.amount_x = self.amount_x.checked_sub(amount_out).context("overflow")?;
        }

        Ok(SwapResult {
            amount_in_with_fees,
            amount_out,
            fee,
            protocol_fee_after_host_fee,
            host_fee,
            is_exact_out_amount: false,
        })
    }
}
