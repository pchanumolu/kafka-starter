package com.example;

import kafka.producer.Partitioner;

/**
 * Created by pchanumolu on 3/31/15.
 */

/**
 * The logic takes the key, which we expect to be the IP address, finds the last octet and
 * does a modulo operation on the number of partitions defined within Kafka for the topic.
 * The benefit of this partitioning logic is all messages from the same source IP end up in the same Partition.
 * Of course so do other IPs, but your consumer logic will need to know how to handle that.
 */
public class PartitionerExample implements Partitioner {
    public PartitionerExample(){}

    @Override
    public int partition(Object key, int numPartitions) {
        int partition = 0;
        String stringKey = (String) key;
        int offSet = stringKey.lastIndexOf('.');
        if (offSet > 0) {
            partition = Integer.parseInt( stringKey.substring(offSet+1)) % numPartitions;
        }
        return partition;
    }
}
