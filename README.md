# hadoop_mapreduce_minmax


##Algorithm

As any Map Reduce program it has two parts map and meduce. In map phase it reads data line by line and generate Key-Value pair for reducer. Here key will be joined Year and Country data e.g. 2010 France. And value will the FX rate.  Next Hadoop does nice job for us by sorting and partitioning this keys together and feeding to the reducer. In the reducer phase  it calculates minimum and maximum fx rates and submits it to the context with new Key-Value pairs for each result i.e. . 1971 Australia MIN.

See full article at: [http://www.khalidmammadov.co.uk/2017/12/18/finding-min-and-max-fx-rates-for-every-country-using-hadoop-mapreduce/](http://www.khalidmammadov.co.uk/2017/12/18/finding-min-and-max-fx-rates-for-every-country-using-hadoop-mapreduce/)