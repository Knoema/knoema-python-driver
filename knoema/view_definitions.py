class Dimension:

    def __init__(self):
        self.key = None
        self.id = None
        self.name = None
        self.isGeo = None
        self.datasetId = None
        self.fields = []
        self.members = None


class Field:

    def __init__(self, field_info):
        self.key = field_info['key']
        self.name = field_info['name']
        self.displayName = field_info['displayName']
        self.type = field_info['type']
        self.locale = field_info['locale']
        self.baseKey = field_info['baseKey']
        self.isSystemField = field_info['isSystemField']
