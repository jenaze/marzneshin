import asyncio
import logging
from datetime import datetime, timedelta

from sqlalchemy import and_, func, or_, update
from sqlalchemy.orm import joinedload

from app import marznode
from app.db import GetDB
from app.db.models import User
from app.models.notification import UserNotification
from app.models.user import (
    UserExpireStrategy,
    UserResponse,
)
from app.notification import notify

logger = logging.getLogger(__name__)


def _get_users_pending_deactivation_condition():
    return and_(
        User.removed == False,
        User.activated == True,
        User.is_active == False,
    )


def _get_users_to_deactivate(db, pending_deactivation_condition):
    return (
        db.query(User)
        .options(
            joinedload(User.inbounds),
            joinedload(User.services),
            joinedload(User.admin),
        )
        .filter(pending_deactivation_condition)
        .all()
    )


def _get_on_hold_users_ready_for_activation(db, now: datetime):
    return (
        db.query(User.id, User.username, User.usage_duration)
        .filter(
            User.removed == False,
            User.expire_strategy == UserExpireStrategy.START_ON_FIRST_USE,
            User.is_active == True,
            or_(
                and_(
                    User.online_at.isnot(None),
                    User.online_at
                    >= func.coalesce(User.edit_at, User.created_at),
                ),
                and_(
                    User.activation_deadline.isnot(None),
                    User.activation_deadline <= now,
                ),
            ),
        )
        .all()
    )


async def review_users():
    now = datetime.utcnow()
    with GetDB() as db:
        pending_deactivation_condition = (
            _get_users_pending_deactivation_condition()
        )
        users_to_deactivate = _get_users_to_deactivate(
            db, pending_deactivation_condition
        )
        deactivated_user_payloads: list[tuple[str, UserResponse]] = []

        if users_to_deactivate:
            db.execute(
                update(User)
                .where(pending_deactivation_condition)
                .values(activated=False),
                execution_options={"synchronize_session": "fetch"},
            )

            for user in users_to_deactivate:
                marznode.operations.update_user(user, remove=True)
                deactivated_user_payloads.append(
                    (
                        user.username,
                        UserResponse.model_validate(user),
                    )
                )

        users_ready_for_activation = _get_on_hold_users_ready_for_activation(
            db, now
        )
        activate_updates = []
        activated_usernames = []
        for user_id, username, usage_duration in users_ready_for_activation:
            if usage_duration is None:
                continue
            activate_updates.append(
                {
                    "id": user_id,
                    "expire_date": now
                    + timedelta(seconds=usage_duration),
                    "expire_strategy": UserExpireStrategy.FIXED_DATE,
                }
            )
            activated_usernames.append(username)

        if activate_updates:
            db.bulk_update_mappings(User, activate_updates)

        if users_to_deactivate or activate_updates:
            db.commit()

        for username, user_payload in deactivated_user_payloads:
            asyncio.ensure_future(
                notify(
                    action=UserNotification.Action.user_deactivated,
                    user=user_payload,
                )
            )
            logger.info(
                "User `%s` activation state changed to `%s`",
                username,
                False,
            )

        for username in activated_usernames:
            logger.info("on hold user `%s` has been activated", username)
