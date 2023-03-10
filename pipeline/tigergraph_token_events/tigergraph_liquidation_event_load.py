from utils.tigergraph_etl import DorisToTigergraphExtractTask
from utils.tigergraph_etl import DorisToTigergraphLoadTask


class LiquidationEventExtract(DorisToTigergraphExtractTask):
    def __init__(self,bucket_size=10000):
        super(LiquidationEventExtract,self).__init__(bucket_size)

    def doris_extract_sql(self,start_block):
        sql = """
        select concat(block_number,'_',log_index) as liquidation_event_id
            ,block_number
            ,log_index
            ,pool_address
            ,platform
            ,liquidator
            ,borrower
            ,liquidator_token_address
            ,liquidator_token_amount
            ,borrow_token_address
            ,borrow_token_amount
        from price_oracle_test.ods_chain_liquidation_events_eth
        where block_number >= {start_block}
        AND block_number < {end_block}
        """.format(
            start_block=start_block,
            end_block=start_block+self.bucket_size
        )
        return sql


def liquidation_event_extract(start_block,bucket_size=10000):
    liquidation_event = LiquidationEventExtract(bucket_size)
    res_data = liquidation_event.extract_doris_data(start_block)
    return res_data


def liquidation_event_tigergraph_load(res_data):
    vertex_liquidation_event_load = DorisToTigergraphLoadTask(
        graph_name="token_events",
        res_data=res_data,
        vertex={
            "vertex_name":"liquidation_event",
            "vertex_attributes":
                ["block_number","log_index","pool_address","platform",
                 "liquidator","borrower",
                 "liquidator_token_address","liquidator_token_amount",
                 "borrow_token_address","borrow_token_amount"
                 ]
        }
    )
    res = vertex_liquidation_event_load.load_to_tigergraph()
    print("vertex_liquidation_event_load",res)


class IsLiquidationEdgeExtract(DorisToTigergraphExtractTask):
    def __init__(self,bucket_size=10000):
        super(IsLiquidationEdgeExtract,self).__init__(bucket_size)

    def doris_extract_sql(self,start_block):
        sql = """
        select concat(block_number,'_',log_index) as liquidation_event_id
            ,transfer_log_indexes
            ,concat(block_number,'_',transfer_log_index.unnest) as transfer_id
        from price_oracle_test.ods_chain_liquidation_events_eth
        ,unnest(split(transfer_log_indexes,',')) transfer_log_index
        WHERE block_number >= {start_block}
        AND block_number < {end_block}
        """.format(
            start_block=start_block,
            end_block=start_block+self.bucket_size
        )
        return sql


def liquidation_edge_extract(start_block,bucket_size=10000):
    liquidation_edge = IsLiquidationEdgeExtract(bucket_size)
    res_data = liquidation_edge.extract_doris_data(start_block)
    return res_data


def liquidation_edge_tigergraph_load(res_data):
    edge_is_liquidation_load = DorisToTigergraphLoadTask(
        graph_name="token_events",
        res_data=res_data,
        edge={
            "edge_name": "is_liquidation",
            "from_vertex_type": "liquidation_event",
            "from_vertex_field_name": "liquidation_event_id",
            "to_vertex_type": "token_transfer",
            "to_vertex_field_name": "transfer_id"
        }
    )
    res = edge_is_liquidation_load.load_to_tigergraph()
    print("edge_is_liquidation_load", res)


def load_liquidation_event(start_block,end_block=999999999,bucket_size=10000):
    while start_block < end_block:
        res_data = liquidation_event_extract(start_block=start_block,bucket_size=bucket_size)
        liquidation_event_tigergraph_load(res_data)
        edge_data = liquidation_edge_extract(start_block=start_block,bucket_size=bucket_size)
        liquidation_edge_tigergraph_load(edge_data)
        start_block += bucket_size
        print(start_block)


def main():
    start_block = 11400000
    end_block = 11900000
    load_liquidation_event(start_block, end_block, bucket_size=500000)


if __name__ == "__main__":
    main()
