from rabbit_niersc import Rabbit

r = Rabbit(profession="unit")


def test_function(**kwargs):
    """
You should always use the same kwargs names as defined in the structure of JSON-message.
On function call they will be passed inside from received message.
    """
    try:
        result = kwargs["a"] + kwargs["b"]
    except TypeError:
        result = 0
    print str(result)
    return result


r.duty = test_function

r.run()
