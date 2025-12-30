class FusionRuntimeError(RuntimeError):
    def __init__(self, message):
        self.message = message

    @property
    def args(self):
        return [self.message]

    def __str__(self):
        return str(self.message)

    def __repr__(self):
        return "FusionRuntimeError: {}".format(self.__str__())