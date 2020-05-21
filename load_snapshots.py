"""Copies data from postgres read replica into `snapshots` schema."""

import json
import logging
import os
import tempfile
from datetime import datetime, timedelta

import boto3
import psycopg2
from psycopg2 import sql

from dd_events import dd_events
from helpers import date_string_or_none, initialize_sentry

def dump(read_replica, redshift, to_file, copy_to,  # pylint: disable=R0913
         query, params=None):
    """Copy a table from the read replica into Redshift."""
    s3_resource = boto3.resource('s3')
    bucket = os.environ.get("DW_BUCKET")

    # Write the file to s3 and add a snapshot_date col to each row
    with read_replica.cursor(name="warehouse_dump") as curs:
        temp = tempfile.NamedTemporaryFile(delete=False)

        curs.execute(query, params)
        # add a snapshot_date col to each row
        for row in curs:
            data = {"snapshot_date": (
                datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")}
            for idx, col in enumerate(curs.description):
                data[col.name] = row[idx]
            line = "{}\n".format(json.dumps(data, default=date_string_or_none))
            temp.write(str.encode(line))

        temp.close()
        s3_resource.Object(bucket, to_file).upload_file(temp.name)
        os.remove(temp.name)

    # import the file into redshift
    with redshift.cursor() as curs:
        curs.execute(
            sql.SQL("""
                COPY {}.{}
                FROM %s
                iam_role %s
                format AS json 'auto'
                timeformat 'YYYY-MM-DDTHH:MI:SS'""").format(
                    sql.Identifier("snapshots"),
                    sql.Identifier(copy_to)
                ), ("s3://{0}/{1}".format(bucket, to_file),
                    os.environ.get("REDSHIFT_ROLE")))
        logging.info("SUCCESS! snapshots.%s table loaded", copy_to)


def main():
    """Copy all scheduled tables."""
    initialize_sentry()
    logging.getLogger().setLevel(logging.INFO)

    read_replica = psycopg2.connect(os.environ.get("REPLICA_DATABASE_URL"))
    redshift = psycopg2.connect(os.environ.get("REDSHIFT_URL"))

    # run all the dumps in a single transaction
    with read_replica:
        day = datetime.today() - timedelta(days=1)

        date_params = (
            "snapshot_year={}/snapshot_month={}/snapshot_day={}".format(
                day.year,
                day.strftime("%m"),
                day.strftime("%d")
            ))

        dump_to = "users/{}/data".format(date_params)
        query = """
            SELECT
                id,
                created_at,
                updated_at,
                deleted_at,
                name
            FROM users
            """

        dump(read_replica, redshift, dump_to, "users", query)

        dump_to = "projects/{}/data".format(date_params)
        query = """
            SELECT
                id,
                repository_id,
                created_at,
                updated_at,
                deleted_at,
                name
            FROM projects"""
        dump(read_replica, redshift, dump_to, "projects", query)


        # incremental activities dump
        dump_to = "activities/{}/incremental".format(date_params)
        query = """
            SELECT
                id,
                event,
                created_at,
                updated_at,
                project_id,
                user_id
            FROM activities
            WHERE DATE(created_at) = %s"""
        dump(read_replica, redshift, dump_to,
             "activities", query, (day.date(),))


    redshift.commit()
    logging.info("Done")

    datadog_events = dd_events("Warehouse Tables")
    dd_events.create(datadog_events)


if __name__ == "__main__":
    main()
