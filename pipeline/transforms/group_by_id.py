from apache_beam import PTransform
from apache_beam import GroupByKey
from apache_beam import Map

class GroupById(PTransform):

    def tag_with_id(self, item):
        return (item.id, item)


    def expand(self, xs):
        return (
            xs
            | Map(self.tag_with_id)
            | "Group by ID" >> GroupByKey()
        )


