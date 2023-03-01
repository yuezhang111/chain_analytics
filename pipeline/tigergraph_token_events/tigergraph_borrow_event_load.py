from utils.tigergraph_etl import DorisToTigergraphExtractTask
from utils.tigergraph_etl import DorisToTigergraphLoadTask


class BorrowEventExtract(DorisToTigergraphExtractTask):
    def __init__(self,bucket_size=10000):
        super(BorrowEventExtract,self).__init__(bucket_size)

    def doris_extract_sql(self,start_block):
        sql = """
        select concat(block_number,'_',id) as borrow_event_id
            ,block_number
            ,log_index
            ,pool_address
            ,platform
            ,borrower
            ,token_address
            ,amount
        from price_oracle_test.ods_chain_debt_events_eth
        where block_number >= {start_block}
        AND block_number < {end_block}
        """.format(
            start_block=start_block,
            end_block=start_block+self.bucket_size
        )
        return sql


def borrow_event_extract(start_block,bucket_size=1000):
    borrow_event = BorrowEventExtract(bucket_size)
    res_data = borrow_event.extract_doris_data(start_block)
    return res_data


def token_transfer_tigergraph_load(res_data):
    vertex_token_transfer_load = DorisToTigergraphLoadTask(
        graph_name="token_events",
        res_data=res_data,
        vertex="token_transfer"
    )

