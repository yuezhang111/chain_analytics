from utils.tigergraph_etl import DorisToTigergraphExtractTask
from utils.tigergraph_etl import DorisToTigergraphLoadTask


class LPEventExtract(DorisToTigergraphExtractTask):
    def __init__(self,bucket_size=10000):
        super(LPEventExtract,self).__init__(bucket_size)

    def doris_extract_sql(self,start_block):
        sql = """
    SELECT concat(a.block_number,'_',a.log_index) as lp_event_id
        ,a.block_number
        ,a.log_index
        ,a.platform
        ,a.pool_address
        ,a.provider
        ,a.mint_or_burn as lp_event_type
        ,b.token_address as lp_token_address
        ,b.amount as lp_token_amount
        ,concat(a.block_number,'_',b.transfer_log_index) as transfer_id
    FROM
    (
        select block_number,log_index,transaction_hash
            ,pool_address,provider,mint_or_burn,platform
        from price_oracle_test.ods_chain_mint_burn_events_eth
        WHERE platform_type = 'swap'
        AND block_number >= {start_block}
        AND block_number < {end_block}
    ) as a
    left join
    (
        SELECT block_number,log_index,token_address,amount
            ,token_type,transfer_log_index,chain_token_holder
        FROM price_oracle_test.ods_chain_mint_burn_details_eth
        WHERE block_number >= {start_block}
            AND block_number < {end_block}
            AND token_type = 1
    ) as b
    ON a.block_number = b.block_number
    AND a.log_index = b.log_index
    WHERE b.transfer_log_index is not null
        """.format(
            start_block=start_block,
            end_block=start_block+self.bucket_size
        )
        return sql


class IsBaseLPExtract(DorisToTigergraphExtractTask):
    def __init__(self,bucket_size=10000):
        super(IsBaseLPExtract,self).__init__(bucket_size)

    def doris_extract_sql(self,start_block):
        sql = """
    SELECT concat(a.block_number,'_',a.log_index) as lp_event_id
        ,concat(a.block_number,'_',b.transfer_log_index) as transfer_id
    FROM
    (
        select block_number,log_index,transaction_hash
            ,pool_address,provider,mint_or_burn,platform
        from price_oracle_test.ods_chain_mint_burn_events_eth
        WHERE platform_type = 'swap'
        AND block_number >= {start_block}
        AND block_number < {end_block}
    ) as a
    left join
    (
        SELECT block_number,log_index,token_address,amount
            ,token_type,transfer_log_index,chain_token_holder
        FROM price_oracle_test.ods_chain_mint_burn_details_eth
        WHERE block_number >= {start_block}
            AND block_number < {end_block}
            AND token_type = 0
    ) as b
    ON a.block_number = b.block_number
    AND a.log_index = b.log_index
    WHERE b.transfer_log_index is not null
        """.format(
            start_block=start_block,
            end_block=start_block+self.bucket_size
        )
        return sql


def lp_event_extract(start_block,bucket_size=1000):
    lp_event = LPEventExtract(bucket_size)
    res_data = lp_event.extract_doris_data(start_block)
    return res_data


def lp_event_tigergraph_load(res_data):
    vertex_lp_event_load = DorisToTigergraphLoadTask(
        graph_name="token_events",
        res_data=res_data,
        vertex={
            "vertex_name":"lp_event",
            "vertex_attributes":
                ["block_number","log_index","platform","pool_address","provider",
                 "lp_event_type","lp_token_address","lp_token_amount"]
        }
    )
    res = vertex_lp_event_load.load_to_tigergraph()
    print("vertex_lp_event_load",res)

    edge_is_lp_token_load = DorisToTigergraphLoadTask(
        graph_name="token_events",
        res_data=res_data,
        edge={
            "edge_name": "is_lp_token",
            "from_vertex_type": "lp_event",
            "from_vertex_field_name": "lp_event_id",
            "to_vertex_type": "token_transfer",
            "to_vertex_field_name": "transfer_id"
        }
    )
    res = edge_is_lp_token_load.load_to_tigergraph()
    print("edge_is_lp_token_load", res)


def is_base_lp_extract(start_block,bucket_size=1000):
    is_base_lp = IsBaseLPExtract(bucket_size)
    res_data = is_base_lp.extract_doris_data(start_block)
    return res_data


def is_base_lp_tigergraph_load(res_data):
    edge_is_base_token_load = DorisToTigergraphLoadTask(
        graph_name="token_events",
        res_data=res_data,
        edge={
            "edge_name": "is_base_lp",
            "from_vertex_type": "lp_event",
            "from_vertex_field_name": "lp_event_id",
            "to_vertex_type": "token_transfer",
            "to_vertex_field_name": "transfer_id"
        }
    )
    res = edge_is_base_token_load.load_to_tigergraph()
    print("edge_is_base_token_load", res)


def load_lp_event(start_block,end_block=999999999,bucket_size=10000):
    while start_block < end_block:
        lp_data = lp_event_extract(start_block=start_block,bucket_size=bucket_size)
        lp_event_tigergraph_load(lp_data)
        base_data = is_base_lp_extract(start_block=start_block, bucket_size=bucket_size)
        is_base_lp_tigergraph_load(base_data)
        start_block += bucket_size
        print(start_block)


def main():
    start_block = 11400000
    end_block = 11500000
    load_lp_event(start_block, end_block, bucket_size=10000)


if __name__ == "__main__":
    main()
