from utils.tigergraph_etl import DorisToTigergraphExtractTask
from utils.tigergraph_etl import DorisToTigergraphLoadTask


class BorrowEventExtract(DorisToTigergraphExtractTask):
    def __init__(self,bucket_size=10000):
        super(BorrowEventExtract,self).__init__(bucket_size)

    def doris_extract_sql(self,start_block):
        sql = """
        select concat(block_number,'_',log_index) as borrow_event_id
            ,block_number
            ,log_index
            ,pool_address
            ,platform
            ,borrower
            ,token_address
            ,amount
            ,concat(block_number,'_',transfer_log_index) as transfer_id
        from price_oracle_test.ods_chain_debt_events_eth
        where borrow_or_repay = 0
        AND block_number >= {start_block}
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


def borrow_event_tigergraph_load(res_data):
    vertex_borrow_event_load = DorisToTigergraphLoadTask(
        graph_name="token_events",
        res_data=res_data,
        vertex={
            "vertex_name":"borrow_event",
            "vertex_attributes":
                ["block_number","log_index","pool_address","platform","borrower","token_address","amount"]
        }
    )
    res = vertex_borrow_event_load.load_to_tigergraph()
    print("vertex_borrow_event_load",res)

    edge_is_borrow_load = DorisToTigergraphLoadTask(
        graph_name="token_events",
        res_data=res_data,
        edge={
            "edge_name": "is_borrow",
            "from_vertex_type": "borrow_event",
            "from_vertex_field_name": "borrow_event_id",
            "to_vertex_type": "token_transfer",
            "to_vertex_field_name": "transfer_id"
        }
    )
    res = edge_is_borrow_load.load_to_tigergraph()
    print("edge_is_borrow_load", res)


def load_borrow_event(start_block,end_block=999999999,bucket_size=10000):
    while start_block < end_block:
        res_data = borrow_event_extract(start_block=start_block,bucket_size=bucket_size)
        borrow_event_tigergraph_load(res_data)
        start_block += bucket_size
        print(start_block)


def main():
    start_block = 11400000
    end_block = 11500000
    load_borrow_event(start_block, end_block)


if __name__ == "__main__":
    main()
