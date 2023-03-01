from utils.tigergraph_etl import DorisToTigergraphExtractTask
from utils.tigergraph_etl import DorisToTigergraphLoadTask


class RepayEventExtract(DorisToTigergraphExtractTask):
    def __init__(self,bucket_size=10000):
        super(RepayEventExtract,self).__init__(bucket_size)

    def doris_extract_sql(self,start_block):
        sql = """
        select concat(block_number,'_',log_index) as repay_event_id
            ,block_number
            ,log_index
            ,pool_address
            ,platform
            ,payer as account_address
            ,payer
            ,borrower
            ,token_address
            ,amount
            ,concat(block_number,'_',transfer_log_index) as transfer_id
        from price_oracle_test.ods_chain_debt_events_eth
        where borrow_or_repay = 1
        AND block_number >= {start_block}
        AND block_number < {end_block}
        """.format(
            start_block=start_block,
            end_block=start_block+self.bucket_size
        )
        return sql


def repay_event_extract(start_block,bucket_size=1000):
    borrow_event = RepayEventExtract(bucket_size)
    res_data = borrow_event.extract_doris_data(start_block)
    return res_data


def repay_event_tigergraph_load(res_data):
    vertex_repay_event_load = DorisToTigergraphLoadTask(
        graph_name="token_events",
        res_data=res_data,
        vertex={
            "vertex_name":"repay_event",
            "vertex_attributes":
                ["block_number","log_index","pool_address","platform",
                 "payer","borrower","token_address","amount"]
        }
    )
    res = vertex_repay_event_load.load_to_tigergraph()
    print("vertex_repay_event_load",res)

    edge_is_repay_load = DorisToTigergraphLoadTask(
        graph_name="token_events",
        res_data=res_data,
        edge={
            "edge_name": "is_repay",
            "from_vertex_type": "repay_event",
            "from_vertex_field_name": "repay_event_id",
            "to_vertex_type": "token_transfer",
            "to_vertex_field_name": "transfer_id"
        }
    )
    res = edge_is_repay_load.load_to_tigergraph()
    print("edge_is_repay_load", res)

    edge_beneficial_to_load = DorisToTigergraphLoadTask(
        graph_name="token_events",
        res_data=res_data,
        edge={
            "edge_name": "beneficial_to",
            "from_vertex_type": "repay_event",
            "from_vertex_field_name": "repay_event_id",
            "to_vertex_type": "account",
            "to_vertex_field_name": "account_address"
        }
    )
    res = edge_beneficial_to_load.load_to_tigergraph()
    print("edge_beneficial_to_load", res)


def load_repay_event(start_block,end_block=999999999,bucket_size=10000):
    while start_block < end_block:
        res_data = repay_event_extract(start_block=start_block,bucket_size=bucket_size)
        repay_event_tigergraph_load(res_data)
        start_block += bucket_size
        print(start_block)


def main():
    start_block = 11400000
    end_block = 11500000
    load_repay_event(start_block, end_block)


if __name__ == "__main__":
    main()
