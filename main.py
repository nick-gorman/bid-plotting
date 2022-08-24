from datetime import timedelta

import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import numpy as np
from nemosis import dynamic_data_compiler

nemosis_data_cache_directory_path = "C:/Users/N.gorman/Documents/nemosis_cache"


def stack_unit_bids(volume_bids, price_bids):
    """Combine volume and price components of offers and reformat them such that each price quantity pair is on a
    separate row of the dataframe."""
    volume_bids = pd.melt(volume_bids, id_vars=['INTERVAL_DATETIME', 'DUID'],
                          value_vars=['BANDAVAIL1', 'BANDAVAIL2', 'BANDAVAIL3', 'BANDAVAIL4',
                                      'BANDAVAIL5', 'BANDAVAIL6', 'BANDAVAIL7', 'BANDAVAIL8',
                                      'BANDAVAIL9', 'BANDAVAIL10'],
                          var_name='BIDBAND', value_name='BIDVOLUME')
    price_bids = pd.melt(price_bids, id_vars=['SETTLEMENTDATE', 'DUID'],
                         value_vars=['PRICEBAND1', 'PRICEBAND2', 'PRICEBAND3', 'PRICEBAND4',
                                     'PRICEBAND5', 'PRICEBAND6', 'PRICEBAND7', 'PRICEBAND8',
                                     'PRICEBAND9', 'PRICEBAND10'],
                         var_name='BIDBAND', value_name='BIDPRICE')
    price_bids['APPLICABLEFROM'] = price_bids['SETTLEMENTDATE'] + timedelta(hours=4, minutes=5)
    price_bids['BIDBAND'] = pd.to_numeric(price_bids['BIDBAND'].str[9:])
    volume_bids['BIDBAND'] = pd.to_numeric(volume_bids['BIDBAND'].str[9:])
    bids = pd.merge_asof(volume_bids.sort_values('INTERVAL_DATETIME'),
                         price_bids.sort_values('APPLICABLEFROM'),
                         left_on='INTERVAL_DATETIME', right_on='APPLICABLEFROM',
                         by=['BIDBAND', 'DUID'])
    return bids


def adjust_bids_for_availability(stacked_bids, unit_availability):
    """Adjust bid volume where the total avaibility bid in would restrict an a bid from actually being dispatched."""
    bids = stacked_bids.sort_values('BIDBAND')
    bids['BIDVOLUMECUMULATIVE'] = bids.groupby(['DUID', 'INTERVAL_DATETIME'], as_index=False)['BIDVOLUME'].cumsum()
    availability = unit_availability.rename({'SETTLEMENTDATE': 'INTERVAL_DATETIME'}, axis=1)
    bids = pd.merge(bids, availability, 'left', on=['INTERVAL_DATETIME', 'DUID'])
    bids['SPAREBIDVOLUME'] = (bids['AVAILABILITY'] - bids['BIDVOLUMECUMULATIVE']) + bids['BIDVOLUME']
    bids['SPAREBIDVOLUME'] = np.where(bids['SPAREBIDVOLUME'] < 0, 0, bids['SPAREBIDVOLUME'])
    bids['ADJUSTEDBIDVOLUME'] = bids[['BIDVOLUME', 'SPAREBIDVOLUME']].min(axis=1)
    return bids


def create_bid_stack_time_series_bar_plot(bids, demand_data):
    """Plot the volume bid accorinding to a set of price range bins. Down sample to bids for dispatch intervals on the
    hour"""
    bids = bids[bids['INTERVAL_DATETIME'].dt.minute.isin([0])]
    bids = bids.sort_values('BIDPRICE')
    bins = [-1000.0, 0.0, 100.0, 300.0, 500.0, 1000.0, 5000.0, 14400.0, 14500.0, 16000.0]
    bids['price_bin'] = pd.cut(bids['BIDPRICE'], bins=bins)
    bids = bids.groupby(['INTERVAL_DATETIME', 'price_bin'], as_index=False).agg({'ADJUSTEDBIDVOLUME': 'sum'})
    fig = px.bar(bids, x='INTERVAL_DATETIME', y='ADJUSTEDBIDVOLUME', color='price_bin')
    demand_data = demand_data.groupby(['SETTLEMENTDATE'], as_index=False).agg({'TOTALDEMAND': 'sum'})
    fig.add_trace(go.Scatter(x=demand_data['SETTLEMENTDATE'], y=demand_data['TOTALDEMAND'],
                             marker=dict(color='blue', size=4), name='demand'))
    fig.update_yaxes(title="Volume (MW)")
    fig.update_xaxes(title="Time (Bid stack sampled on the hour)")
    return fig


def run(start_time, end_time):

    volume_bids = dynamic_data_compiler(start_time=start_time, end_time=end_time, table_name='BIDPEROFFER_D',
                                        raw_data_location=nemosis_data_cache_directory_path,
                                        fformat='parquet', keep_csv=False)
    volume_bids = volume_bids[volume_bids['BIDTYPE'] == 'ENERGY']
    price_bids = dynamic_data_compiler(start_time=start_time, end_time=end_time, table_name='BIDDAYOFFER_D',
                                       raw_data_location=nemosis_data_cache_directory_path,
                                       fformat='parquet')
    price_bids = price_bids[price_bids['BIDTYPE'] == 'ENERGY']
    availability = dynamic_data_compiler(start_time=start_time, end_time=end_time, table_name='DISPATCHLOAD',
                                         raw_data_location=nemosis_data_cache_directory_path,
                                         fformat='parquet', select_columns=['INTERVENTION', 'SETTLEMENTDATE', 'DUID',
                                                                            'AVAILABILITY'])
    availability = availability[availability['INTERVENTION'] == 0]
    stacked_bids = stack_unit_bids(volume_bids, price_bids)
    stacked_bids = adjust_bids_for_availability(stacked_bids, availability)

    demand_data = dynamic_data_compiler(start_time=start_time, end_time=end_time,
                                        raw_data_location=nemosis_data_cache_directory_path,
                                        table_name='DISPATCHREGIONSUM', fformat='parquet',
                                        select_columns=['REGIONID', 'TOTALDEMAND', 'SETTLEMENTDATE', 'INTERVENTION'])
    demand_data = demand_data[demand_data['INTERVENTION'] == 0]
    demand_data = demand_data.loc[:, ['SETTLEMENTDATE', 'REGIONID', 'TOTALDEMAND']]

    complete_bid_stack_plot_bar = create_bid_stack_time_series_bar_plot(stacked_bids, demand_data)

    complete_bid_stack_plot_bar.write_html("bids_all_units_plot_bar.html")


if __name__ == '__main__':
    run(start_time='2019/01/21 00:00:00', end_time='2019/01/28 00:00:00')




