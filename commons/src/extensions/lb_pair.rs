use crate::*;
use anchor_spl::token::spl_token;
use anchor_spl::token_2022::spl_token_2022;
use ruint::aliases::U1024;
use solana_sdk::pubkey::Pubkey;
use std::ops::Deref;
use std::ops::Shl;
use std::ops::Shr;

pub trait LbPairExtension {
    fn bitmap_range() -> (i32, i32);
    fn get_bin_array_offset(bin_array_index: i32) -> usize;

    fn status(&self) -> Result<PairStatusWrapper>;
    fn pair_type(&self) -> Result<PairTypeWrapper>;
    fn activation_type(&self) -> Result<ActivationTypeWrapper>;
    fn compute_fee(&self, amount: u64) -> Result<u64>;
    fn get_total_fee(&self) -> Result<u128>;
    fn get_base_fee(&self) -> Result<u128>;
    fn get_variable_fee(&self) -> Result<u128>;
    fn get_token_programs(&self) -> Result<[Pubkey; 2]>;
    fn compute_variable_fee(&self, volatility_accumulator: u32) -> Result<u128>;
    fn compute_protocol_fee(&self, fee_amount: u64) -> Result<u64>;
    fn compute_fee_from_amount(&self, amount_with_fees: u64) -> Result<u64>;
    fn is_overflow_default_bin_array_bitmap(&self, bin_array_index: i32) -> bool;
    fn next_bin_array_index_with_liquidity_internal(
        &self,
        swap_for_y: bool,
        start_array_index: i32,
    ) -> Result<(i32, bool)>;

    fn update_references(&mut self, current_timestamp: i64) -> Result<()>;
    fn update_volatility_accumulator(&mut self) -> Result<()>;
    fn advance_active_bin(&mut self, swap_for_y: bool) -> Result<()>;
}

impl LbPairExtension for LbPair {
    fn status(&self) -> Result<PairStatusWrapper> {
        Ok(self.status.try_into()?)
    }

    fn get_token_programs(&self) -> Result<[Pubkey; 2]> {
        let mut token_programs_id = [Pubkey::default(); 2];

        for (i, token_program_flag) in [
            self.token_mint_x_program_flag,
            self.token_mint_y_program_flag,
        ]
        .into_iter()
        .enumerate()
        {
            let flag: TokenProgramFlagWrapper = token_program_flag.try_into()?;
            let token_program_id = match flag.deref() {
                TokenProgramFlags::TokenProgram => spl_token::ID,
                TokenProgramFlags::TokenProgram2022 => spl_token_2022::ID,
            };
            token_programs_id[i] = token_program_id;
        }

        Ok(token_programs_id)
    }

    fn pair_type(&self) -> Result<PairTypeWrapper> {
        Ok(self.pair_type.try_into()?)
    }

    fn activation_type(&self) -> Result<ActivationTypeWrapper> {
        Ok(self.activation_type.try_into()?)
    }

    // 更新用于计算动态费用的参考值
    // 在 Meteora DLMM 中，交易手续费的一部分是根据市场波动性动态调整的，而这个函数就是实现该动态调整机制的关键一步
    fn update_references(&mut self, current_timestamp: i64) -> Result<()> {
        //v_params (v_parameters)：指的是可变参数 (Variable Parameters)。
        //这些是随着市场活动（如交易）而频繁变化的参数，例如上次更新的时间戳、波动率累加器等
        let v_params = &mut self.v_parameters;
        //s_params (parameters)：指的是静态参数 (Static Parameters)。
        //这些是创建流动性池时就已设定的、通常不会改变的参数，例如各种费率、时间周期等
        let s_params = &self.parameters;

        //计算距离上一次成功更新过去了多长时间
        let elapsed = current_timestamp
            .checked_sub(v_params.last_update_timestamp)
            .context("overflow")?;

        // Not high frequency trade
        //只有当经过的时间 elapsed 超过了预设的 filter_period（过滤周期），
        //函数内的逻辑才会执行。这可以防止在高频交易场景下过于频繁地更新参考值，起到节流的作用
        if elapsed >= s_params.filter_period as i64 {
            //如果满足更新条件，它会立刻将当前活跃的流动性仓位ID (self.active_id)
            //保存到 index_reference 中。这个 index_reference 就像一个快照，
            //记录了在这次更新时间点的市场价格位置。后续计算波动性时，
            //就可以通过比较新的 active_id 和这个 index_reference 的差距来判断价格变动了多少
            v_params.index_reference = self.active_id;
            //根据 elapsed 的时间长度来处理波动率参考值 (volatility_reference)
            //如果 elapsed 大于 filter_period 但小于 decay_period（衰减周期），则进入“衰减”模式
            if elapsed < s_params.decay_period as i64 {
                //当前的 volatility_accumulator（波动率累加器）乘以一个 reduction_factor（衰减因子，一个小于1的系数）。
                //这会使累积的波动率值随着时间的推移而逐渐降低或“衰减”
                let volatility_reference = v_params
                    .volatility_accumulator
                    .checked_mul(s_params.reduction_factor as u32)
                    .context("overflow")?
                    .checked_div(BASIS_POINT_MAX as u32)
                    .context("overflow")?;
                //衰减后的新值被存入 volatility_reference
                v_params.volatility_reference = volatility_reference;
            }
            // 如果 elapsed 连 decay_period 都超过了，说明市场已经很长时间没有满足更新条件的活动
            //在这种情况下，代码会直接将 volatility_reference 重置为 0。这意味着之前累积的波动率已经过时，失去了参考价值，因此被完全清除。
            else {
                v_params.volatility_reference = 0;
            }
            // volatility_reference的作用:
            //提供一个随时间衰减的波动性基准值。它作为计算当前实时波动率 (volatility_accumulator) 的起始点或底数
        }

        Ok(())
    }

    //实时计算和更新池子的“波动率累加器”
    //这个累加器是 Meteora DLMM 动态费用机制的核心，它的值越高，交易者需要支付的可变费用（Variable Fee）就越多
    //简单来说，这个函数在交易过程中的每一步（每消耗一个 Bin 的流动性）都会被调用
    //更完整解释可以看笔记
    fn update_volatility_accumulator(&mut self) -> Result<()> {
        //v_params.index_reference: 这是上一次“参考点更新”（通过 update_references 函数）时记录的价格快照（当时的 active_id）。
        let v_params = &mut self.v_parameters;
        let s_params = &self.parameters;

        let delta_id = i64::from(v_params.index_reference)
            .checked_sub(self.active_id.into())
            .context("overflow")?
            .unsigned_abs();

        let volatility_accumulator = u64::from(v_params.volatility_reference)
            .checked_add(
                delta_id
                    .checked_mul(BASIS_POINT_MAX as u64)
                    .context("overflow")?,
            )
            .context("overflow")?;

        v_params.volatility_accumulator = std::cmp::min(
            volatility_accumulator,
            s_params.max_volatility_accumulator.into(),
        )
        .try_into()
        .context("overflow")?;

        Ok(())
    }

    fn get_base_fee(&self) -> Result<u128> {
        Ok(u128::from(self.parameters.base_factor)
            .checked_mul(self.bin_step.into())
            .context("overflow")?
            .checked_mul(10u128)
            .context("overflow")?
            .checked_mul(10u128.pow(self.parameters.base_fee_power_factor.into()))
            .context("overflow")?)
    }

    fn get_variable_fee(&self) -> Result<u128> {
        self.compute_variable_fee(self.v_parameters.volatility_accumulator)
    }

    fn compute_variable_fee(&self, volatility_accumulator: u32) -> Result<u128> {
        if self.parameters.variable_fee_control > 0 {
            let volatility_accumulator: u128 = volatility_accumulator.into();
            let bin_step: u128 = self.bin_step.into();
            let variable_fee_control: u128 = self.parameters.variable_fee_control.into();

            let square_vfa_bin = volatility_accumulator
                .checked_mul(bin_step)
                .context("overflow")?
                .checked_pow(2)
                .context("overflow")?;

            let v_fee = variable_fee_control
                .checked_mul(square_vfa_bin)
                .context("overflow")?;

            let scaled_v_fee = v_fee
                .checked_add(99_999_999_999)
                .context("overflow")?
                .checked_div(100_000_000_000)
                .context("overflow")?;

            return Ok(scaled_v_fee);
        }

        Ok(0)
    }

    fn get_total_fee(&self) -> Result<u128> {
        let total_fee_rate = self
            .get_base_fee()?
            .checked_add(self.get_variable_fee()?)
            .context("overflow")?;
        let total_fee_rate_cap = std::cmp::min(total_fee_rate, MAX_FEE_RATE.into());
        Ok(total_fee_rate_cap)
    }

    fn compute_fee(&self, amount: u64) -> Result<u64> {
        let total_fee_rate = self.get_total_fee()?;
        let denominator = u128::from(FEE_PRECISION)
            .checked_sub(total_fee_rate)
            .context("overflow")?;

        // Ceil division
        let fee = u128::from(amount)
            .checked_mul(total_fee_rate)
            .context("overflow")?
            .checked_add(denominator)
            .context("overflow")?
            .checked_sub(1)
            .context("overflow")?;

        let scaled_down_fee = fee.checked_div(denominator).context("overflow")?;

        Ok(scaled_down_fee.try_into().context("overflow")?)
    }

    fn advance_active_bin(&mut self, swap_for_y: bool) -> Result<()> {
        let next_active_bin_id = if swap_for_y {
            self.active_id.checked_sub(1)
        } else {
            self.active_id.checked_add(1)
        }
        .context("overflow")?;

        ensure!(
            next_active_bin_id >= MIN_BIN_ID && next_active_bin_id <= MAX_BIN_ID,
            "Insufficient liquidity"
        );

        self.active_id = next_active_bin_id;

        Ok(())
    }

    fn compute_protocol_fee(&self, fee_amount: u64) -> Result<u64> {
        let protocol_fee = u128::from(fee_amount)
            .checked_mul(self.parameters.protocol_share.into())
            .context("overflow")?
            .checked_div(BASIS_POINT_MAX as u128)
            .context("overflow")?;

        Ok(protocol_fee.try_into().context("overflow")?)
    }

    fn compute_fee_from_amount(&self, amount_with_fees: u64) -> Result<u64> {
        let total_fee_rate = self.get_total_fee()?;

        let fee_amount = u128::from(amount_with_fees)
            .checked_mul(total_fee_rate)
            .context("overflow")?
            .checked_add((FEE_PRECISION - 1).into())
            .context("overflow")?;

        let scaled_down_fee = fee_amount
            .checked_div(FEE_PRECISION.into())
            .context("overflow")?;

        Ok(scaled_down_fee.try_into().context("overflow")?)
    }

    fn bitmap_range() -> (i32, i32) {
        (-BIN_ARRAY_BITMAP_SIZE, BIN_ARRAY_BITMAP_SIZE - 1)
    }

    fn is_overflow_default_bin_array_bitmap(&self, bin_array_index: i32) -> bool {
        let (min_bitmap_id, max_bitmap_id) = Self::bitmap_range();
        bin_array_index > max_bitmap_id || bin_array_index < min_bitmap_id
    }

    fn get_bin_array_offset(bin_array_index: i32) -> usize {
        (bin_array_index + BIN_ARRAY_BITMAP_SIZE) as usize
    }

    fn next_bin_array_index_with_liquidity_internal(
        &self,
        swap_for_y: bool,
        start_array_index: i32,
    ) -> Result<(i32, bool)> {
        let bin_array_bitmap = U1024::from_limbs(self.bin_array_bitmap);
        let array_offset: usize = Self::get_bin_array_offset(start_array_index);
        let (min_bitmap_id, max_bitmap_id) = LbPair::bitmap_range();
        if swap_for_y {
            let bitmap_range: usize = max_bitmap_id
                .checked_sub(min_bitmap_id)
                .context("overflow")?
                .try_into()
                .context("overflow")?;
            let offset_bit_map =
                bin_array_bitmap.shl(bitmap_range.checked_sub(array_offset).context("overflow")?);

            if offset_bit_map.eq(&U1024::ZERO) {
                return Ok((min_bitmap_id.checked_sub(1).context("overflow")?, false));
            } else {
                let next_bit = offset_bit_map.leading_zeros();
                return Ok((
                    start_array_index
                        .checked_sub(next_bit as i32)
                        .context("overflow")?,
                    true,
                ));
            }
        } else {
            let offset_bit_map = bin_array_bitmap.shr(array_offset);
            if offset_bit_map.eq(&U1024::ZERO) {
                return Ok((max_bitmap_id.checked_add(1).context("overflow")?, false));
            } else {
                let next_bit = offset_bit_map.trailing_zeros();
                return Ok((
                    start_array_index
                        .checked_add(next_bit as i32)
                        .context("overflow")?,
                    true,
                ));
            };
        }
    }
}
