from typing import Any, Optional
from dataclasses import dataclass


@dataclass
class Base:
    type: str

    def list_for_parse(self):
        raise NotImplemented

    def parse_attributes_to(self, result: dict):
        raise NotImplemented

    @staticmethod
    def to_tiger_graph_dict(entities: list) -> dict:
        result = dict()
        for entity in entities:
            r = result
            for item in entity.list_for_parse():
                if item not in r:
                    r[item] = dict()
                r = r[item]
            entity.parse_attributes_to(r)
        return result


@dataclass
class Vertex(Base):
    id: Any
    attributes: Optional[dict]

    def parse_attributes_to(self, result: dict):
        for attribute in self.attributes:
            result[attribute] = {'value': self.attributes[attribute]}

    def list_for_parse(self) -> list:
        return [self.type, self.id]

    def get_unique_id(self) -> str:
        return "{}#{}".format(self.type, self.id)


@dataclass
class Edge(Base):
    from_type: str
    from_id: Any
    to_type: str
    to_id: Any
    attributes: Optional[dict]

    def parse_attributes_to(self, result: dict):
        for k, v in self.attributes.items():
            result[k] = {'value': v}

    def list_for_parse(self) -> list:
        return [self.from_type, self.from_id, self.type, self.to_type, self.to_id]

    def get_unique_id(self) -> str:
        return "{}#{}#{}#{}#{}".format(self.from_type, self.from_id, self.type, self.to_type, self.to_id)
