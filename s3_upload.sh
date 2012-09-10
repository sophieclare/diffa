#!/bin/sh

BUCKET=s3://diffa-builds

s3cmd put --acl-public --guess-mime-type dist/target/*.zip $BUCKET
s3cmd put --acl-public --guess-mime-type agent/target/*.war $BUCKET
