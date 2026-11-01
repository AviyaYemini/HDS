import uuid

from sqlalchemy import select

from hds.models.organisation import Organisation
from hds.repositories.base import BaseRepository


class OrganisationRepository(BaseRepository[Organisation]):
    model = Organisation

    async def get_by_id(self, id: uuid.UUID) -> Organisation | None:
        result = await self.session.execute(select(Organisation).where(Organisation.id == id))
        return result.scalar_one_or_none()
