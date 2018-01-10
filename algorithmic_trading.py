from quantopian.algorithm import attach_pipeline, pipeline_output
from quantopian.pipeline import Pipeline
from quantopian.pipeline import CustomFactor
from quantopian.pipeline.data.builtin import USEquityPricing
from quantopian.pipeline.data import morningstar
from quantopian.pipeline.filters.morningstar import Q1500US
import pandas as pd
import numpy as np

def initialize(context):
    # set commision and slippage
    set_commission(commission.PerShare(cost=0.0075, min_trade_cost=1))
    set_slippage(slippage.VolumeShareSlippage(volume_limit=0.025, price_impact=0.1))
    
    # initialize the leverage
    context.leverage = 1.00
    
    # initialize the portion
    context.long_portion = 0.0
    context.short_portion = 0.0
    
    context.active = [sid(22739), sid(22972), sid(22446), sid(23881)]
    context.bond = sid(23870)
    context.gold = sid(26807)
    
    context.assets = set(context.active + [context.bond])
    context.channels = [60, 120, 180, 252]
    context.entry = 0.60
    context.exit = 0.10
    
    context.alloc = pd.Series([0.0] * len(context.assets), index=context.assets)
    
    context.modes = {}
    for s in context.active:
        for l in context.channels:
            context.modes[(s, l)] = 0
    
    
    schedule_function(func=reallocate, 
                      date_rule=date_rules.every_day(), 
                      time_rule=time_rules.market_open(hours=0,minutes=15), 
                      half_days=True)
    
    schedule_function(func=rebalance, 
                      date_rule=date_rules.every_day(), 
                      time_rule=time_rules.market_open(hours=0,minutes=30), 
                      half_days=True)
    
    schedule_function(func=record_vars,
                      date_rule=date_rules.every_day(),
                      time_rule=time_rules.market_close(),
                      half_days=True)
    
    attach_pipeline(make_pipeline(context), name = 'my_pipeline')
    
def make_pipeline(context):
    pipe = Pipeline()
    
    #Add factors defined to the pipeline
    earnings_yield = EarningsYield()
    price_to_sale = PriceToSale()
    ebitda_yield = EbitdaYield()     
    debt_ratio = DebtRatio()
    super_yield = SuperYield()
    adj_book_ratio = AdjBookRatio()
    
    classification_restr = (morningstar.asset_classification.morningstar_industry_code.latest != 10319042) |\
                           (morningstar.asset_classification.morningstar_industry_code.latest != 20532078)
    
    # List of the universe of stocks that we care about
    tradable_stock = Q1500US() & classification_restr
    
    # Rank factors and add the rank to our pipeline
    earnings_yield_rank = earnings_yield.rank(mask = tradable_stock)              
    price_to_sale_rank = price_to_sale.rank(mask = tradable_stock)              
    ebitda_yield_rank = ebitda_yield.rank(mask = tradable_stock)
    debt_ratio_rank = debt_ratio.rank(mask = tradable_stock)
    super_yield_rank = super_yield.rank(mask = tradable_stock)
    
    adj_book_ratio_rank = adj_book_ratio.rank(mask = tradable_stock)
    
    # Take the average of the factors rankings, add this to the pipeline
    combo_raw = (earnings_yield_rank + price_to_sale_rank +
                ebitda_yield_rank + debt_ratio_rank +
                super_yield_rank + adj_book_ratio_rank)
    
    # Rank the combo_raw and add that to the pipeline
    pipe.add(combo_raw.rank(mask=tradable_stock), 'combo_rank')
    
    pipe.set_screen(tradable_stock)
    
    return pipe

def before_trading_start(context, data):
    
    context.output = pipeline_output('my_pipeline')
    context.output = context.output.dropna()
    
    context.long_df = context.output.sort_values(by = 'combo_rank', ascending=False).iloc[:20]
    context.short_df = context.output.sort_values(by = 'combo_rank', ascending=False).iloc[-20:]
    context.cash_etf_list = [context.bond]
    
def record_vars(context, data):
    
    # Record and plot the leverage of our portfolio over time. 
    record(leverage = context.account.leverage)
    record(long_portion = context.long_portion)
    record(short_portion = context.short_portion)
    
    print "Long List"
    log.info("\n" + str(context.long_df.sort_values(by = 'combo_rank', ascending=True).head(10)))
    
    print "Short List" 
    log.info("\n" + str(context.short_df.sort_values(by = 'combo_rank', ascending=True).head(10)))
    
# This rebalancing is called according to our schedule_function settings.     
def rebalance(context, data):
    context.long_portion = context.alloc.iloc[0] + context.alloc.iloc[1] +\
                           context.alloc.iloc[2] + context.alloc.iloc[4]
    
    context.short_portion = context.alloc.iloc[3]
    
    each_short_weight = context.short_portion * 0.5
    # each_short_weight = context.short_portion
    
    long_weight = float(context.long_portion) / float(len(context.long_df))
    short_weight = each_short_weight / float(len(context.short_df))
    cash_etf_weight = each_short_weight / float(len(context.cash_etf_list))
    
    print "the long weight: " +  str(long_weight)
    print "the short weight: " +  str(short_weight)
    print "the cash etf weight: " + str(cash_etf_weight)
    
    # universe now contains just longs and shorts
    context.security_df = context.long_df.append(context.short_df)
    
    for long_stock in context.long_df.index:
        if data.can_trade(long_stock):
            log.info("ordering longs for " + str(long_stock))
            log.info("weight is %s" % (long_weight))
            order_target_percent(long_stock, long_weight)
    
    for short_stock in context.short_df.index:
        if data.can_trade(short_stock):
            log.info("ordering shorts for " + str(short_stock))
            log.info("weight is %s" % (short_weight))
            order_target_percent(short_stock, - short_weight)
    
    for cash_etf in context.cash_etf_list:
        if data.can_trade(cash_etf):
            log.info("ordering cash ETF for " + str(cash_etf))
            log.info("weight is %s" % (cash_etf_weight))
            order_target_percent(cash_etf, cash_etf_weight)
    
    # make sure all untradeable securities are sold off each day
    for stock in context.portfolio.positions:
        if stock not in context.security_df:
            if data.can_trade(stock):
                order_target_percent(stock, 0)
       
class EarningsYield(CustomFactor):
    
    inputs = [morningstar.valuation_ratios.pe_ratio]
    window_length = 1
    
    def compute(self, today, assets, out, pe_ratio):
        out[:] = 1 / pe_ratio
        
        
class PriceToSale(CustomFactor):

    inputs = [morningstar.valuation_ratios.ps_ratio]
    window_length = 1
    
    def compute(self, today, assets, out, ps_ratio):       
        out[:] = 1 / ps_ratio
        
        
class EbitdaYield(CustomFactor):
    
    inputs = [morningstar.income_statement.ebitda,\
             morningstar.valuation.enterprise_value]
    window_length = 1
    
    def compute(self, today, assets, out, ebitda, enterprise_value):
        out[:] = ebitda / enterprise_value

        
class DebtRatio(CustomFactor):
    
    inputs = [morningstar.valuation.market_cap,\
             morningstar.balance_sheet.total_debt]
    window_length = 1
    
    def compute(self, today, assets, out, market_cap, total_debt):
        out[:] = market_cap / total_debt

class SuperYield(CustomFactor):
    
    inputs = [morningstar.valuation_ratios.fcf_yield]
    window_length = 1
    
    def compute(self, today, assets, out, fcf_yield):
        out[:] = fcf_yield
    
class AdjBookRatio(CustomFactor):
    inputs = [morningstar.balance_sheet.total_assets,\
              morningstar.valuation.market_cap,\
              morningstar.balance_sheet.common_stock_equity]
    window_length = 1
    
    def compute(self, today, assets, out, total_assets, market_cap, common_stock_equity):
        adjbook = total_assets + (0.1 * (market_cap - common_stock_equity))
        out[:] = adjbook / market_cap
    
def reallocate(context, data):
    h = data.history(context.active, 'price', 300, '1d')
    hs = h.ix[-20:]
    p = h.ix[-1]

    hvol = 1.0 / hs.pct_change().std()
    hvol_all = hvol.sum()
    r = (hvol / hvol_all) * 1.0 / len(context.channels)
    alloc = pd.Series([0.0] * len(context.assets), index=context.assets)
    
    
    for l in context.channels:
        values = h.ix[-l:]
        entry = values.quantile(context.entry)
        exit = values.quantile(context.exit)
        
        
        for s in context.active:
            m = (s, l)
            if context.modes[m] == 0 and p[s] >= entry[s]:
                print "entry: %s/%d" % (s.symbol, l)
                context.modes[m] = 1
            elif context.modes[m] == 1 and p[s] <= exit[s]:
                print "exit: %s/%d" % (s.symbol, l)
                context.modes[m] = 0
                
            if context.modes[m] == 0:
                alloc[context.bond] += r[s]
            elif context.modes[m] == 1:
                alloc[s] += r[s]
            
    context.alloc = alloc
    
    print "Allocation:\n%s" % context.alloc