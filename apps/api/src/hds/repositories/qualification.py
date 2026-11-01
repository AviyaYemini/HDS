import uuid
from typing import Any

from sqlalchemy import delete, select

from hds.models.qualification import Qualification, user_qualifications
from hds.repositories.base import BaseRepository


class QualificationRepository(BaseRepository[Qualification]):
    model = Qualification

    async def list_by_org(self, org_id: uuid.UUID) -> list[Qualification]:
        result = await self.session.execute(
            select(Qualification)
            .where(Qualification.organization_id == org_id)
            .order_by(Qualification.name)
        )
        return list(result.scalars().all())

    async def get_by_name_and_org(self, name: str, org_id: uuid.UUID) -> Qualification | None:
        result = await self.session.execute(
            select(Qualification).where(
                Qualification.name == name,
                Qualification.organization_id == org_id,
            )
        )
        return result.scalar_one_or_none()

    async def create(self, org_id: uuid.UUID, name: str) -> Qualification:
        qual = Qualification(id=uuid.uuid4(), organization_id=org_id, name=name)
        self.session.add(qual)
        await self.session.commit()
        await self.session.refresh(qual)
        return qual

    async def update(self, id: uuid.UUID, **fields: Any) -> Qualification:
        from sqlalchemy import update as sa_update

        await self.session.execute(
            sa_update(Qualification).where(Qualification.id == id).values(**fields)
        )
        await self.session.commit()
        qual = await self.get_by_id(id)
        assert qual is not None
        return qual

    async def delete(self, id: uuid.UUID) -> None:
        qual = await self.get_by_id(id)
        if qual is not None:
            await self.session.delete(qual)
            await self.session.commit()

    async def get_qualifications_for_user(self, user_id: uuid.UUID) -> list[Qualification]:
        result = await self.session.execute(
            select(Qualification)
            .join(
                user_qualifications,
                Qualification.id == user_qualifications.c.qualification_id,
            )
            .where(user_qualifications.c.user_id == user_id)
        )
        return list(result.scalars().all())

    async def replace_user_qualifications(
        self, user_id: uuid.UUID, qual_ids: list[uuid.UUID]
    ) -> None:
        await self.session.execute(
            delete(user_qualifications).where(user_qualifications.c.user_id == user_id)
        )
        for qid in qual_ids:
            await self.session.execute(
                user_qualifications.insert().values(user_id=user_id, qualification_id=qid)
            )
        await self.session.commit()
