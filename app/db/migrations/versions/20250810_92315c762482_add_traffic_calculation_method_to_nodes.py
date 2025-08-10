"""add traffic calculation method to nodes

Revision ID: 92315c762482
Revises: 57eba0a293f2
Create Date: 2025-08-10 00:30:34.758137

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '92315c762482'
down_revision = '57eba0a293f2'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column('nodes', sa.Column('traffic_calculation_method', sa.String(length=32), server_default='sum', nullable=False))


def downgrade() -> None:
    op.drop_column('nodes', 'traffic_calculation_method')
