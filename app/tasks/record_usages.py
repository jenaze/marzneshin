import asyncio
from collections import defaultdict
from datetime import datetime

from sqlalchemy import and_, select, insert, update, bindparam

from app import marznode
from app.db import GetDB
from app.db.models import NodeUsage, NodeUserUsage, User
from app.marznode import MarzNodeBase
from app.tasks.data_usage_percent_reached import data_usage_percent_reached


def record_user_usage_logs(params: list, node_id: int):
    if not params:
        return

    created_at = datetime.fromisoformat(
        datetime.utcnow().strftime("%Y-%m-%dT%H:00:00")
    )

    with GetDB() as db:
        # make user usage row if it doesn't exist
        select_stmt = select(NodeUserUsage.user_id).where(
            and_(
                NodeUserUsage.node_id == node_id,
                NodeUserUsage.created_at == created_at,
            )
        )
        existings = [r[0] for r in db.execute(select_stmt).fetchall()]
        uids_to_insert = set()

        for p in params:
            uid = p["uid"]
            if uid in existings:
                continue
            uids_to_insert.add(uid)

        if uids_to_insert:
            stmt = insert(NodeUserUsage).values(
                user_id=bindparam("uid"),
                created_at=created_at,
                node_id=node_id,
                used_traffic=0,
            )
            db.execute(stmt, [{"uid": uid} for uid in uids_to_insert])

        # record
        stmt = (
            update(NodeUserUsage)
            .values(
                used_traffic=NodeUserUsage.used_traffic + bindparam("value")
            )
            .where(
                and_(
                    NodeUserUsage.user_id == bindparam("uid"),
                    NodeUserUsage.node_id == node_id,
                    NodeUserUsage.created_at == created_at,
                )
            )
        )
        db.connection().execute(
            stmt,
            [
                {
                    "uid": usage["uid"],
                    "value": int(usage.get("value") or 0),
                }
                for usage in params
            ],
            execution_options={"synchronize_session": None},
        )
        db.commit()


def record_node_stats(node_id: int, usage: int):
    if not usage:
        return

    created_at = datetime.fromisoformat(
        datetime.utcnow().strftime("%Y-%m-%dT%H:00:00")
    )

    with GetDB() as db:
        # make node usage row if doesn't exist
        select_stmt = select(NodeUsage.node_id).where(
            and_(
                NodeUsage.node_id == node_id,
                NodeUsage.created_at == created_at,
            )
        )
        notfound = db.execute(select_stmt).first() is None
        if notfound:
            stmt = insert(NodeUsage).values(
                created_at=created_at, node_id=node_id, uplink=0, downlink=0
            )
            db.execute(stmt)

        # record
        stmt = (
            update(NodeUsage)
            .values(
                downlink=NodeUsage.downlink + usage,
            )
            .where(
                and_(
                    NodeUsage.node_id == node_id,
                    NodeUsage.created_at == created_at,
                )
            )
        )

        db.execute(stmt)
        db.commit()


from app.models.node import TrafficCalculationMethod


async def get_users_stats(
    node_id: int, node: MarzNodeBase
) -> tuple[int, list[dict]]:
    try:
        params = list()
        for stat in await asyncio.wait_for(node.fetch_users_stats(), 10):
            uplink = getattr(stat, "uplink", 0)
            downlink = getattr(stat, "downlink", 0)
            if hasattr(stat, "usage"):
                # For backward compatibility with older marznode versions
                # TODO: V2 - Remove this
                if stat.usage:
                    params.append({"uid": stat.uid, "value": stat.usage})
            elif uplink or downlink:
                params.append(
                    {"uid": stat.uid, "uplink": uplink, "downlink": downlink}
                )
        return node_id, params
    except:
        return node_id, []


def _calculate_usage(param, traffic_method):
    if "value" in param:
        # For backward compatibility with older marznode versions
        # TODO: V2 - Remove this
        return param["value"]
    else:
        if traffic_method == TrafficCalculationMethod.SUM:
            return param["uplink"] + param["downlink"]
        elif traffic_method == TrafficCalculationMethod.UPLINK_ONLY:
            return param["uplink"]
        elif traffic_method == TrafficCalculationMethod.DOWNLINK_ONLY:
            return param["downlink"]
        else:
            return param["uplink"] + param["downlink"]


async def record_user_usages():
    # usage_coefficient = {None: 1}  # default usage coefficient for the main api instance

    results = await asyncio.gather(
        *[
            get_users_stats(node_id, node)
            for node_id, node in marznode.nodes.items()
        ]
    )
    api_params = {node_id: params for node_id, params in list(results)}

    users_usage = defaultdict(int)
    for node_id, params in api_params.items():
        node = marznode.nodes.get(node_id)
        if not node:
            continue
        coefficient = node.usage_coefficient
        traffic_method = node.traffic_calculation_method

        node_usage = 0
        for param in params:
            value = _calculate_usage(param, traffic_method)
            users_usage[param["uid"]] += int(
                value * coefficient
            )  # apply the usage coefficient
            node_usage += value
        record_node_stats(node_id, node_usage)

    users_usage = list(
        {"id": uid, "value": value} for uid, value in users_usage.items()
    )
    if not users_usage:
        return

    # record users usage
    with GetDB() as db:
        await data_usage_percent_reached(db, users_usage)

        stmt = update(User).values(
            used_traffic=User.used_traffic + bindparam("value"),
            lifetime_used_traffic=User.lifetime_used_traffic
            + bindparam("value"),
            online_at=datetime.utcnow(),
        )

        db.execute(
            stmt, users_usage, execution_options={"synchronize_session": None}
        )
        db.commit()

    for node_id, params in api_params.items():
        node = marznode.nodes.get(node_id)
        if not node:
            continue
        coefficient = node.usage_coefficient
        traffic_method = node.traffic_calculation_method

        for param in params:
            param["value"] = _calculate_usage(
                param, traffic_method
            ) * coefficient
        record_user_usage_logs(
            params,
            node_id,
        )
