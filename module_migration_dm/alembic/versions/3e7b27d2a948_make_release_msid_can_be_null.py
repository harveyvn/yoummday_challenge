"""make release_msid can be null

Revision ID: 3e7b27d2a948
Revises: 07b195c708f6
Create Date: 2025-07-04 18:47:08.089802

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '3e7b27d2a948'
down_revision: Union[str, Sequence[str], None] = '07b195c708f6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column('tracks', 'release_msid',
               existing_type=postgresql.UUID(),
               nullable=True)
    # ### end Alembic commands ###


def downgrade() -> None:
    """Downgrade schema."""
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column('tracks', 'release_msid',
               existing_type=postgresql.UUID(),
               nullable=False)
    # ### end Alembic commands ###
