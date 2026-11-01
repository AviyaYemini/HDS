import uuid
from datetime import UTC, datetime

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from hds.models.invitation_token import InvitationToken


class InvitationTokenRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def create(
        self,
        user_id: uuid.UUID,
        token_hash: str,
        purpose: str,
        expires_at: datetime,
    ) -> InvitationToken:
        token = InvitationToken(
            id=uuid.uuid4(),
            user_id=user_id,
            token_hash=token_hash,
            purpose=purpose,
            expires_at=expires_at,
            created_at=datetime.now(UTC),
        )
        self.session.add(token)
        await self.session.commit()
        await self.session.refresh(token)
        return token

    async def get_by_hash(self, token_hash: str) -> InvitationToken | None:
        result = await self.session.execute(
            select(InvitationToken).where(InvitationToken.token_hash == token_hash)
        )
        return result.scalar_one_or_none()

    async def mark_used(self, id: uuid.UUID) -> None:
        await self.session.execute(
            update(InvitationToken)
            .where(InvitationToken.id == id)
            .values(used_at=datetime.now(UTC))
        )
        await self.session.commit()
