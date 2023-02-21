CREATE OR REPLACE Distributed QUERY nft_balance_cost(
  SET<VERTEX<account>> address,
  BOOL print_edge=FALSE,
  INT debug_mode_level=0
)
FOR GRAPH nft_profit {
  /***
  # debug_mode_level = 0 : (default)production output
  # debug_mode_level = 1 : seed_size iteration check
  # debug_mode_level = 2 : seed detail check
  # debug_mode_level > 2 : midstep res detail check
  ***/

  TYPEDEF TUPLE <
      INT block_number,
      INT token_num,
      INT trade_type,
      VERTEX<nft_transfer> v>
  order_value;

  TYPEDEF TUPLE <
      STRING nft_address,
      STRING nft_id>
  nft_key;

  MapAccum<nft_key, SumAccum<INT>> @nft_balance;
  MapAccum<nft_key, MaxAccum<INT>> @nft_balance_block;
  HeapAccum<order_value>(100,block_number DESC,v DESC) @nft_in_sorted;
  MapAccum<nft_key, ListAccum<order_value>> @get_records;
  MapAccum<nft_key, ListAccum<order_value>> @matched_records;
  MapAccum<nft_key, SumAccum<DOUBLE>> @nft_balance_cost;
  SetAccum<Vertex<nft_transfer>> @@not_finish;
  SetAccum<Vertex<nft_transfer>> @local_not_finish;
  SumAccum<INT> @valid_num = -1;

  MapAccum<nft_key,SumAccum<DOUBLE>> @@acct_nft_balance;
  MapAccum<nft_key,SumAccum<DOUBLE>> @@acct_nft_balance_cost;
  MapAccum<nft_key,SumAccum<DOUBLE>> @@acct_nft_balance_gas;
  SetAccum<Vertex<nft_transfer>> @@buy_nodes;
  SetAccum<Vertex<nft_transfer>> @@transfer_nodes;

  SetAccum<EDGE> @@print_edges;

  # 示例地址：
  # Base case with transfer: 0x40f085994b3208850a1776f4b0ab55205a82b134
  # vopa: 0x2329a3412ba3e49be47274a72383839bbf1cde88
  # pranksy: 0xd387a6e4e84a6c86bd90c158c6028a58cc8ac459

  /**************************************************/
  # 计算s.@nft_balance
  /**************************************************/
  start = address;
  r0 = SELECT s
       FROM start:s-(reverse_receive_nft:e)-nft_transfer:t
       ACCUM
         s.@nft_balance += (
            nft_key(t.token_address, t.token_id) -> t.token_num
         ),
         s.@nft_balance_block += (
           nft_key(t.token_address, t.token_id) -> t.block_number
         )
  ;
  r0 = SELECT s
       FROM start:s-(send_nft:e)-nft_transfer:t
       ACCUM
         s.@nft_balance += (
           nft_key(t.token_address, t.token_id) -> -1*t.token_num
         )
       POST-ACCUM
         FOREACH (nft,balance_value) IN s.@nft_balance DO
           IF balance_value <= 0 THEN
              s.@nft_balance.remove(nft)
              #,s.@nft_balance_block.remove(nft)
           ELSE
              @@acct_nft_balance += (nft -> balance_value)
           END
         END
  ;
  IF debug_mode_level > 1 THEN
    PRINT r0;
  END;

  /**************************************************/
  # 计算s.@nft_balance对应的入手Cost
  /**************************************************/

  seed = address;
  int seed_size = 0;
  seed_size = seed.size();
  WHILE seed_size > 0 LIMIT 50 DO
    # Query#1::根据balance中的nft,匹配合法的买入或转入,存入@matched_records
    get_trades = SELECT t FROM seed:s-(reverse_receive_nft:e)-nft_transfer:t
                 WHERE s.@nft_balance.containsKey(nft_key(t.token_address, t.token_id))
                 AND (t.from_address != t.to_address OR t.trade_type > 0)
                 ACCUM
                    # 将nft_balance中未匹配的nft所有买入或者转入分别存入@get_records中
                    s.@get_records += (
                      nft_key(t.token_address, t.token_id) ->
                      order_value(t.block_number,t.token_num,t.trade_type,t)
                    )
                  POST-ACCUM
                    # 对每个nft,找到该账户最近balance_value笔转入的transfer或trade
                    FOREACH (nft,balance_value) IN s.@nft_balance DO
                      # 将nft对应的<nft_transfer>加入s.@nft_in_sorted排序
                      FOREACH odr IN s.@get_records.get(nft) DO
                        s.@nft_in_sorted += odr
                      END,
                      # 输出balance中对应的最近几次入手<nft_transfer>
                      INT bal = balance_value,
                      WHILE bal > 0 AND s.@nft_in_sorted.size()>0 DO
                        ## pop出最近一次合法的入手nft <nft_transfer>
                        IF s.@nft_in_sorted.top().block_number > s.@nft_balance_block.get(nft) THEN
                          s.@nft_in_sorted.pop(),
                          BREAK
                        END,
                        INT in_token_num = s.@nft_in_sorted.top().token_num,
                        INT in_trade_type = s.@nft_in_sorted.top().trade_type,
                        INT in_block_number = s.@nft_in_sorted.top().block_number,
                        VERTEX<nft_transfer> get_record_v = s.@nft_in_sorted.pop().v,
                        INT match_num = in_token_num,
                        ## 更新结果
                        s.@local_not_finish += get_record_v,
                        IF in_trade_type >= 1 THEN
                          @@buy_nodes += get_record_v
                        ELSE
                          @@transfer_nodes += get_record_v
                        END,
                        IF bal >= in_token_num THEN
                          bal = bal - in_token_num
                        ELSE
                          match_num = bal,
                          bal = 0
                        END,
                        s.@matched_records += (
                          nft -> order_value(in_block_number,match_num,in_trade_type,get_record_v)
                        )
                      END,
                      # 清空@nft_in_sorted
                      s.@nft_in_sorted.clear()
                    END
    ;
    # Query2::根据@matched_records,更新balance_cost
    get_res = SELECT t FROM seed:s-(reverse_receive_nft:e)-nft_transfer:t
              WHERE s.@local_not_finish.contains(t)
              ACCUM
                FOREACH odr in s.@matched_records.get(nft_key(t.token_address,t.token_id)) DO
                  IF odr.v == t and odr.trade_type == 0 THEN
                    @@print_edges += e,
                    t.@valid_num = odr.token_num,
                    @@acct_nft_balance_gas += (
                      nft_key(t.token_address,t.token_id) -> t.gas_cost
                    ),
                    @@not_finish += t,
                    BREAK
                  ELSE IF odr.v == t and odr.trade_type >= 1 THEN
                    @@print_edges += e,
                    t.@valid_num = odr.token_num,
                    @@acct_nft_balance_cost += (
                      nft_key(t.token_address,t.token_id) -> t.buyer_pay_amt * odr.token_num / t.token_num
                    ),
                    @@acct_nft_balance_gas += (
                      nft_key(t.token_address,t.token_id) -> t.gas_cost
                    ),
                    BREAK
                  END
                END
    ;
    IF debug_mode_level > 2 THEN
      PRINT get_res;
    END;
    reset_collection_accum(@nft_balance);
    reset_collection_accum(@nft_balance_block);
    reset_collection_accum(@get_records);
    reset_collection_accum(@matched_records);
    reset_collection_accum(@local_not_finish);

    # Query3::根据transfer更新下一次循环的seed accounts
    next_step = @@not_finish;
    @@not_finish.clear();
    seed = SELECT t FROM next_step:s-(reverse_send_nft:e)-account:t
           ACCUM
             t.@nft_balance += (nft_key(s.token_address,s.token_id) -> s.@valid_num),
             t.@nft_balance_block += (nft_key(s.token_address,s.token_id) -> s.block_number),
             @@print_edges += e
    ;
    seed_size = seed.size();

    IF debug_mode_level == 1 THEN
      PRINT seed.size();
    ELSE IF debug_mode_level > 1 AND seed_size < 5 THEN
      PRINT seed;
    END;

  END;

  IF print_edge THEN
    PRINT @@print_edges;
  END;

  SumAccum<DOUBLE> @@total_cost = 0;
  FOREACH (nft,cost) IN @@acct_nft_balance_cost DO
    @@total_cost += cost;
  END;
  PRINT @@total_cost;

  IF debug_mode_level == 0 THEN
    PRINT @@acct_nft_balance;
    PRINT @@acct_nft_balance_cost;
    PRINT @@acct_nft_balance_gas;
  END;
}