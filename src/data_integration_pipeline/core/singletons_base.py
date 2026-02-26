class SingletonBase:
    """Generic Singleton base class to support multiple unique child instances."""

    _instances = {}

    def __new__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(SingletonBase, cls).__new__(cls)
        return cls._instances[cls]
