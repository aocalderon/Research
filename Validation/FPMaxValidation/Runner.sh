#!/usr/bin/bash

echo "Starting..."

nohup spark-submit --class FPMaxTester /home/acald013/Research/PFlock/target/scala-2.11/pflock_2.11-2.0.jar --path Validation/FPMaxValidation/D1/ --dataset Datasets_berlin0-10_110.0_5_6_5 --extension txt &

nohup spark-submit --class FPMaxTester /home/acald013/Research/PFlock/target/scala-2.11/pflock_2.11-2.0.jar --path Validation/FPMaxValidation/D1/ --dataset Datasets_berlin0-10_110.0_5_6_6 --extension txt &

nohup spark-submit --class FPMaxTester /home/acald013/Research/PFlock/target/scala-2.11/pflock_2.11-2.0.jar --path Validation/FPMaxValidation/D1/ --dataset Datasets_berlin0-10_110.0_5_6_7 --extension txt &

nohup spark-submit --class FPMaxTester /home/acald013/Research/PFlock/target/scala-2.11/pflock_2.11-2.0.jar --path Validation/FPMaxValidation/D1/ --dataset Datasets_berlin0-10_110.0_5_6_8 --extension txt &

nohup spark-submit --class FPMaxTester /home/acald013/Research/PFlock/target/scala-2.11/pflock_2.11-2.0.jar --path Validation/FPMaxValidation/D1/ --dataset Datasets_berlin0-10_110.0_5_6_9 --extension txt &

nohup spark-submit --class FPMaxTester /home/acald013/Research/PFlock/target/scala-2.11/pflock_2.11-2.0.jar --path Validation/FPMaxValidation/D1/ --dataset Datasets_berlin0-10_110.0_5_6_10 --extension txt &

echo "Done!!!"
