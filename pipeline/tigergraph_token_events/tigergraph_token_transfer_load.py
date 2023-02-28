from utils.tigergraph_etl import DorisToTigergraphExtractTask
from utils.tigergraph_etl import DorisToTigergraphLoadTask


class TokenTransferExtract(DorisToTigergraphExtractTask):
    def __init__(self,bucket_size=10000):
        super(TokenTransferExtract,self).__init__(bucket_size)

    def doris_extract_sql(self,start_block):
        sql = """
    SELECT concat(block_number,'_',id) as transfer_id
        ,transaction_hash as txn_hash
        ,block_number
        ,id as log_index
        ,from_address
        ,to_address
        ,token_address
        ,amount
        ,transfer_time
        ,IFNULL(symbol,'') as symbol
    FROM dw.dwb_token_transfer_detail_eth_hi
    WHERE block_number >= {start_block}
    AND block_number < {end_block}
    AND source = 'transfer'
    AND token_address not in (
        '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
        '0xdac17f958d2ee523a2206206994597c13d831ec7',
        '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48',
        '0x6b175474e89094c44da98b954eedeac495271d0f'
    )
        """.format(
            start_block=start_block,
            end_block=start_block+self.bucket_size
        )
        return sql


def token_transfer_extract(start_block,bucket_size=1000):
    token_transfer = TokenTransferExtract(bucket_size)
    res_data = token_transfer.extract_doris_data(start_block)
    return res_data


def token_transfer_tigergraph_load(res_data):
    vertex_token_transfer_load = DorisToTigergraphLoadTask(
        graph_name="token_events",
        res_data=res_data,
        vertex="token_transfer"
    )
    res = vertex_token_transfer_load.load_to_tigergraph()
    print("vertex_token_transfer_load",res)

    edge_send_token_load = DorisToTigergraphLoadTask(
        graph_name="token_events",
        res_data=res_data,
        edge={
            "edge_name":"send_token",
            "from_vertex_type":"account",
            "from_vertex_field_name":"from_address",
            "to_vertex_type":"token_transfer",
            "to_vertex_field_name":"transfer_id"
        }
    )
    res = edge_send_token_load.load_to_tigergraph()
    print("edge_send_token_load",res)

    edge_receive_token_load = DorisToTigergraphLoadTask(
        graph_name="token_events",
        res_data=res_data,
        edge={
            "edge_name": "receive_token",
            "from_vertex_type": "token_transfer",
            "from_vertex_field_name": "transfer_id",
            "to_vertex_type": "account",
            "to_vertex_field_name": "from_address"
        }
    )
    res = edge_receive_token_load.load_to_tigergraph()
    print("edge_receive_token_load", res)


def main():
    start_block = 16000000
    bucket_size = 1000
    end_block = 16010000
    while start_block < end_block:
        res_data = token_transfer_extract(start_block=start_block,bucket_size=bucket_size)
        token_transfer_tigergraph_load(res_data)
        start_block += bucket_size
        print((start_block-16000000)/bucket_size + 1)


if __name__ == "__main__":
    main()
