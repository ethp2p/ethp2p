# Attestation Propagation

## Goal
Get the 1m to 1m propagation time down to 10-20 seconds in total.
This would get us a (2-3) slots finality. And can be adopted as is in a chain where the availability chain is decoupled from the finality chain. 

## Layer 1: Validators attesting to a block
There are 1m validators, 10k beacon nodes. 
Say there are 
`num_committees` committees
`committees_per_beacon_node` commitees a single beacon node subscribes to

Total data sent in the network is roughly:

```
total_data_per_committee = (num_validators / num_committees) * ((num_beacon_node * 
committees_per_beacon_node) / num_committees) * (forwarding_factor * attestation_size)


total_data  = total_data_per_committee * num_committees
            = num_validators * forwarding_factor * attestation_size * ((num_beacon_node *
                 committees_per_beacon_node) / num_committees)


total_data_per_beacon_node = total_data / num_beacon_nodes
                           = num_validators * attestation_size * forwarding_factor *
                           (commitees_per_beacon_node / num_committees)
```

For our purposes in this layer we're bandwidth limited. Assume that we have 1million
validators and `num_committees` = 128, `committees_per_beacon_node = 1`. 

```
1M * 200 * 8 *  1 / 128 ~= 12.5 MB
12.5 * 8 / 25 Mbps = 4 seconds
```
our total transmit time here is 4 seconds. 

```
total_data_per_beacon_node = total_data / num_beacon_nodes
                           = num_validators * attestation_size * forwarding_factor *
                           (commitees_per_beacon_node / num_committees)
```

- if we increase the number of committees, total data per beacon node decreases. This is to be expected because there are fewer validators sending on that committee. 
- We cant have too many committees. There's a limit to the maximum number of committees we have. 
```
num_nodes_in_committee = num_beacon_nodes * committees_per_beacon_node / num_committees
```
This `num_nodes_in_committee >= 64`
- Note if we have twice the committees and reduce the committees a single beacon node subscribes to (committees_per_beacon_node) there's no impact on the bandwidth usage. 
  - So we should keep to single committee per beacon node because that would give us better aggregations as there are twice the validators aggregated in a single committee.
- The simulation numbers below suggests that we can potentially get 8000 validators aggregated in the lower layer, and keep the current two layer design. Layer 1 aggregates in a committee, Layer 2 is just the global aggregations topic where everyone publishes. 


### Some simulation numbers

```
  Run A — partial_512_f4000_d8_d4_v2 (mesh 512, fanout 4000/topic × 2 topics, 4000 attestors/topic)

  ┌───────────────────────┬────────────────┬─────────────────┐
  │                       │      D=8       │ D=4 (ihave off) │
  ├───────────────────────┼────────────────┼─────────────────┤
  │ p50 time-to-95%       │ 1120 ms        │ 1164 ms         │
  ├───────────────────────┼────────────────┼─────────────────┤
  │ p90                   │ 1366 ms        │ 1220 ms         │
  ├───────────────────────┼────────────────┼─────────────────┤
  │ max                   │ 4197 ms        │ 1654 ms         │
  ├───────────────────────┼────────────────┼─────────────────┤
  │ sent / mesh node      │ ~33 MB         │ ~12 MB          │
  ├───────────────────────┼────────────────┼─────────────────┤
  │ peak sent super / reg │ ~90 / ~43 Mbps │ ~32 / ~27 Mbps  │
  └───────────────────────┴────────────────┴─────────────────┘

  Run B — partial_80_f8000_d8_d4_1topic_v2 (mesh 80, fanout 8000, 1 topic, 8000 attestors)

  ┌───────────────────────┬────────────────┬─────────────────┐
  │                       │      D=8       │ D=4 (ihave off) │
  ├───────────────────────┼────────────────┼─────────────────┤
  │ p50                   │ 1047 ms        │ 1064 ms         │
  ├───────────────────────┼────────────────┼─────────────────┤
  │ p90                   │ 1173 ms        │ 1128 ms         │
  ├───────────────────────┼────────────────┼─────────────────┤
  │ max                   │ 1408 ms        │ 1247 ms         │
  ├───────────────────────┼────────────────┼─────────────────┤
  │ sent / mesh node      │ ~33 MB         │ ~12 MB          │
  ├───────────────────────┼────────────────┼─────────────────┤
  │ peak sent super / reg │ ~83 / ~34 Mbps │ ~30 / ~33 Mbps  │
  └───────────────────────┴────────────────┴─────────────────┘

```

### problem here
In simulations we are seeing propagation times of 1second. That makes no sense. It should take about
2 seonds to transmit all the data.
Some of it is because we send less that mesh_degree because 1. we never send to the peer we receive from. 2 we might receive from multiple peers in one partial messages tick. 3. batching attestations helps a lot.

