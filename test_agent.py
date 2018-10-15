from rabbit_niersc import Rabbit

r = Rabbit(host="localhost", profession="agent", name="test", data_type="test_data")

# notice that we didn't specify any functions for r.duty method
# so every time this rabbit receives a message it will notice us that it has nothing to do

r.run()
