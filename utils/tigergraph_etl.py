import json
import requests
from loguru import logger
from collections import defaultdict
from utils.doris_connector import DorisClient
from utils.tigergraph_connector import tg_prod_host
from utils.tigergraph_connector import tg_prod_port
from utils.tigergraph_connector import TigergraphClient
from utils.tigergraph_base import Vertex
from utils.tigergraph_base import Edge


def check_and_parse(result: requests.Response):
    if result.status_code != 200:
        logger.error("code: {}, body: {}", result.status_code, result.text)
        raise Exception(result.status_code)
    data = json.loads(result.text, strict=False)
    x = data.get('error', False)
    if x == 'false':
        x = False
    if x:
        raise Exception(result.text)
    return data


class DorisToTigergraphExtractTask(object):
    def __init__(self,bucket_size=10000):
        self.bucket_size = bucket_size

    def doris_extract_sql(self,start_block):
        raise NotImplemented

    def extract_doris_data(self, start_block):
        db = DorisClient("dw")
        sql = self.doris_extract_sql(start_block)
        res_rows, field_names = db.read_sql(sql)
        print(len(res_rows))
        return [res_rows, field_names]


class DorisToTigergraphLoadTask(object):
    def __init__(self, graph_name,res_data,is_test=True,vertex=None,edge=None):
        """
        :param graph_name:
        :param vertex: string - vertex_name
        :param edge: dict - {edge_name:,from_vertex_type:,from_vertex_field_name:,to_vertex_type:,to_vertex_field_name:}
        :param res_data:
        """
        self.graph_name = graph_name
        self.vertex = vertex
        self.edge = edge
        self.is_test = is_test
        self.res_rows = res_data[0]
        self.field_names = res_data[1]

    def transform_vertex_data(self):
        """
        all fields in res_data is vertex attribute
        :return: formatted vertices dict to insert
        """
        vertices_dict = defaultdict(dict)
        for row in self.res_rows:
            attribute_dict = dict()
            for j in range(1, len(row)):
                attribute_dict[self.field_names[j]] = row[j]
            Vertex(self.vertex,row[0],attribute_dict).parse_attributes_to(vertices_dict[row[0]])
        return {'vertices':{self.vertex:vertices_dict}}

    def transform_edge_data(self):
        """
        no attribute for edge in this version
        :return: formatted edge dict to insert
        """
        from_vertex_type = self.edge["from_vertex_type"]
        from_vertex_field_name = self.edge["from_vertex_field_name"]
        to_vertex_type = self.edge["to_vertex_type"]
        to_vertex_field_name = self.edge["to_vertex_field_name"]
        from_vertex_index = self.field_names.index(from_vertex_field_name)
        to_vertex_index = self.field_names.index(to_vertex_field_name)
        edge_list = []
        for row in self.res_rows:
            from_vertex_id = row[from_vertex_index]
            to_vertex_id = row[to_vertex_index]
            edge_list.append(
                Edge(self.edge["edge_name"],from_vertex_type,from_vertex_id,to_vertex_type,to_vertex_id,{})
            )
        return {"edges":Edge.to_tiger_graph_dict(edge_list)}

    def load_to_tigergraph(self):
        if self.is_test:
            tg = TigergraphClient(self.graph_name)
        else:
            tg = TigergraphClient(self.graph_name,host=tg_prod_host,rest_port=tg_prod_port)
        if self.vertex is not None:
            vertex_entities = self.transform_vertex_data()
            vertex_load = json.dumps(vertex_entities,default=str)
            v_dict = json.loads(vertex_load)
            res = tg.insert(v_dict)
            return res.text
        if self.edge is not None:
            edge_entities = self.transform_edge_data()
            edge_load = json.dumps(edge_entities,default=str)
            e_dict = json.loads(edge_load)
            res = tg.insert(e_dict)
            return res.text
