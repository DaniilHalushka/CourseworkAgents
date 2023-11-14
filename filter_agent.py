import asyncio

from neo4j import AsyncGraphDatabase


class Neo4jContextRequestManager:
    def __init__(self, url, username, password, database_name):
        self.painting_name = None
        self.URL = url
        self.USERNAME = username
        self.PASSWORD = password
        self.DATABASE_NAME = database_name
        self.author = ""
        self.date = ""
        self.driver = None

    async def connect_to_database(self):
        self.driver = AsyncGraphDatabase.driver(
            self.URL, auth=(self.USERNAME, self.PASSWORD), database=self.DATABASE_NAME
        )

    async def get_painting_name(self):
        print("sojfwniej")
        query = "MATCH (p:Node)<-[:nrel_context]-(cr:Class) RETURN p.name LIMIT 1"
        print("sojfwniej")

        record = await self.driver.execute_query(query)

        self.painting_name = record.records[0]['p.name']

        print("sojfwniej")

    async def sort_and_remove_extra_context_requests(self):
        print("ojqpef")
        await self.connect_to_database()
        print("efohqo")
        await self.get_painting_name()
        print("fwmoefo")

        if not self.painting_name:
            print("Не удалось получить картину.")
            return

        async with self.driver.session() as session:
            await session.write_transaction(self._sort_and_remove_extra_context_requests)

    async def _sort_and_remove_extra_context_requests(self, tx):
        query = (
                "MATCH (p:Node {name: '" + self.painting_name + "'})<-[:nrel_context]-(cr:Class) RETURN cr ORDER BY ID(cr)"
        )
        result = await self.driver.execute_query(
            "MATCH (p:Node {name: '" + self.painting_name + "'})<-[:nrel_context]-(cr:Class) RETURN cr ORDER BY ID(cr)"
        )
        records = result.records

        if len(records) > 1:
            first_node_id = records[0]['cr'].id

            for record in records[1:]:
                cr_node = record['cr']
                await self._remove_context_relation(tx, cr_node)

    async def _remove_context_relation(self, tx, context_request_node):
        query = (
                "MATCH (p:Node {name: '" + self.painting_name + "'})<-[r:nrel_context]-(cr:Class) WHERE ID(cr) <> " + str(
            context_request_node.id) + " DELETE r"
        )
        await tx.run(query, painting_name=self.painting_name, context_request_id=context_request_node.id)


async def main():
    uri = "bolt://localhost:7687"
    user = "DANIIL"
    password = "12345678"
    database = "vlad1"

    manager = Neo4jContextRequestManager(uri, user, password, database)
    await manager.sort_and_remove_extra_context_requests()


if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(main())
