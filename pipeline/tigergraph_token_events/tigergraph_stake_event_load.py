from utils.tigergraph_etl import DorisToTigergraphExtractTask
from utils.tigergraph_etl import DorisToTigergraphLoadTask


class StakeEventExtract(DorisToTigergraphExtractTask):
    def __init__(self,bucket_size=10000):
        super(StakeEventExtract,self).__init__(bucket_size)

    def doris_extract_sql(self,start_block):
        sql = """
            SELECT concat(a.block_number,'_',a.log_index) as stake_event_id
                ,a.block_number
                ,a.log_index
                ,a.platform
                ,a.pool_address
                ,a.provider
                ,a.mint_or_burn as stake_event_type
                ,b.token_address as stake_token_address
                ,b.amount as stake_token_amount
                ,concat(a.block_number,'_',b.transfer_log_index) as transfer_id
            FROM
            (
                select block_number,log_index,transaction_hash
                    ,pool_address,provider,mint_or_burn,platform
                from price_oracle_test.ods_chain_mint_burn_events_eth
                WHERE platform_type = 'loan'
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
            end_block=start_block + self.bucket_size
        )
        return sql


class IsBaseStakeExtract(DorisToTigergraphExtractTask):
    def __init__(self,bucket_size=10000):
        super(IsBaseStakeExtract,self).__init__(bucket_size)

    def doris_extract_sql(self,start_block):
        sql = """
    SELECT concat(a.block_number,'_',a.log_index) as stake_event_id
        ,concat(a.block_number,'_',b.transfer_log_index) as transfer_id
    FROM
    (
        select block_number,log_index,transaction_hash
            ,pool_address,provider,mint_or_burn,platform
        from price_oracle_test.ods_chain_mint_burn_events_eth
        WHERE platform_type = 'loan'
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


def stake_event_extract(start_block,bucket_size=1000):
    stake_event = StakeEventExtract(bucket_size)
    res_data = stake_event.extract_doris_data(start_block)
    return res_data


def stake_event_tigergraph_load(res_data):
    vertex_stake_event_load = DorisToTigergraphLoadTask(
        graph_name="token_events",
        res_data=res_data,
        vertex={
            "vertex_name":"stake_event",
            "vertex_attributes":
                ["block_number","log_index","platform","pool_address","provider",
                 "stake_event_type","stake_token_address","stake_token_amount"]
        }
    )
    res = vertex_stake_event_load.load_to_tigergraph()
    print("vertex_stake_event_load",res)

    edge_is_stake_token_load = DorisToTigergraphLoadTask(
        graph_name="token_events",
        res_data=res_data,
        edge={
            "edge_name": "is_stake_token",
            "from_vertex_type": "stake_event",
            "from_vertex_field_name": "stake_event_id",
            "to_vertex_type": "token_transfer",
            "to_vertex_field_name": "transfer_id"
        }
    )
    res = edge_is_stake_token_load.load_to_tigergraph()
    print("edge_is_stake_token_load", res)


def is_base_stake_extract(start_block,bucket_size=1000):
    is_base_stake = IsBaseStakeExtract(bucket_size)
    res_data = is_base_stake.extract_doris_data(start_block)
    return res_data


def is_base_stake_tigergraph_load(res_data):
    edge_is_base_token_load = DorisToTigergraphLoadTask(
        graph_name="token_events",
        res_data=res_data,
        edge={
            "edge_name": "is_base_stake",
            "from_vertex_type": "stake_event",
            "from_vertex_field_name": "stake_event_id",
            "to_vertex_type": "token_transfer",
            "to_vertex_field_name": "transfer_id"
        }
    )
    res = edge_is_base_token_load.load_to_tigergraph()
    print("edge_is_base_stake_load", res)


def load_stake_event(start_block,end_block=999999999,bucket_size=10000):
    while start_block < end_block:
        stake_data = stake_event_extract(start_block=start_block,bucket_size=bucket_size)
        stake_event_tigergraph_load(stake_data)
        base_data = is_base_stake_extract(start_block=start_block, bucket_size=bucket_size)
        is_base_stake_tigergraph_load(base_data)
        start_block += bucket_size
        print(start_block)


def main():
    start_block = 11400000
    end_block = 11500000
    load_stake_event(start_block, end_block, bucket_size=10000)


if __name__ == "__main__":
    main()
