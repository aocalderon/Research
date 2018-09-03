#!/usr/bin/bash

echo "Expansion size: 25"
spark-submit ~/Research/PFlock/target/scala-2.11/pflock_2.11-2.0.jar --dataset berlin0-10 --epsilon 110 --epsilon_max 110 --mu 5 --mu_max 5 --delta 6 --delta_max 6 --cores 28 --expansion_size 25

echo "Expansion size: 50"
spark-submit ~/Research/PFlock/target/scala-2.11/pflock_2.11-2.0.jar --dataset berlin0-10 --epsilon 110 --epsilon_max 110 --mu 5 --mu_max 5 --delta 6 --delta_max 6 --cores 28 --expansion_size 50

echo "Expansion size: 75"
spark-submit ~/Research/PFlock/target/scala-2.11/pflock_2.11-2.0.jar --dataset berlin0-10 --epsilon 110 --epsilon_max 110 --mu 5 --mu_max 5 --delta 6 --delta_max 6 --cores 28 --expansion_size 75

echo "Expansion size: 100"
spark-submit ~/Research/PFlock/target/scala-2.11/pflock_2.11-2.0.jar --dataset berlin0-10 --epsilon 110 --epsilon_max 110 --mu 5 --mu_max 5 --delta 6 --delta_max 6 --cores 28 --expansion_size 100

echo "Expansion size: 125"
spark-submit ~/Research/PFlock/target/scala-2.11/pflock_2.11-2.0.jar --dataset berlin0-10 --epsilon 110 --epsilon_max 110 --mu 5 --mu_max 5 --delta 6 --delta_max 6 --cores 28 --expansion_size 125

echo "Expansion size: 150"
spark-submit ~/Research/PFlock/target/scala-2.11/pflock_2.11-2.0.jar --dataset berlin0-10 --epsilon 110 --epsilon_max 110 --mu 5 --mu_max 5 --delta 6 --delta_max 6 --cores 28 --expansion_size 150

echo "Expansion size: 175"
spark-submit ~/Research/PFlock/target/scala-2.11/pflock_2.11-2.0.jar --dataset berlin0-10 --epsilon 110 --epsilon_max 110 --mu 5 --mu_max 5 --delta 6 --delta_max 6 --cores 28 --expansion_size 175

echo "Expansion size: 200"
spark-submit ~/Research/PFlock/target/scala-2.11/pflock_2.11-2.0.jar --dataset berlin0-10 --epsilon 110 --epsilon_max 110 --mu 5 --mu_max 5 --delta 6 --delta_max 6 --cores 28 --expansion_size 200 
