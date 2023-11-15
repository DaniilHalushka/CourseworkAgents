from neo4j import AsyncGraphDatabase
import asyncio


class SearchingAgentByTitleAndAuthor:
    def __init__(self, url, username, password, database_name):
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

    async def close_database_connection(self):
        if self.driver:
            await self.driver.close()

    async def get_initial_data(self):
        author_query = "MATCH (n: Text)<-[:nrel_author]-(m: Node {name: 'current_request'}) RETURN n.name"
        date_query = "MATCH (n: Text)<-[:nrel_date]-(m: Node {name: 'current_request'}) RETURN n.name"

        author_record = await self.driver.execute_query(author_query)
        date_record = await self.driver.execute_query(date_query)

        self.author = author_record.records[0]["n.name"] if author_record.records else ""
        self.date = date_record.records[0]["n.name"] if date_record.records else ""

    async def execute_query_and_return_names(self, query):
        record = await self.driver.execute_query(query)
        return [record["ns"]._properties["name"] for record in record.records]

    async def find_picture_by_author(self, author):
        query = f"MATCH (n: Node {{name: '{author}'}})-[:nrel_write]->(ns: Node) RETURN ns"
        return await self.execute_query_and_return_names(query)

    async def find_picture_by_date(self, date):
        query = f"MATCH (n: Node)-[:nrel_date]->(ns: Node {{name: '{date}'}}) RETURN n"
        return await self.execute_query_and_return_names(query)

    async def find_picture_by_author_and_date(self, author, date):
        query = f"MATCH (n: Node {{name: '{author}'}})-[:nrel_write]->(ns: Node)-[:nrel_date]->(d: Node {{name: '{date}'}}) RETURN ns"
        return await self.execute_query_and_return_names(query)

    async def find_picture(self, author="", date=""):
        if author and not date:
            names = await self.find_picture_by_author(author)
        elif date and author:
            names = await self.find_picture_by_author_and_date(author, date)
        else:
            names = await self.find_picture_by_date(date)
        return names

    async def create_relationship_to_req(self, name):
        query = f"MATCH (p: Node {{name: '{name}'}}), (req: Node {{name: 'current_request'}}) CREATE (req)-[:nrel_context]->(p)"
        await self.driver.execute_query(query)

    async def add_picture_to_req_context(self):
        await self.connect_to_database()
        await self.get_initial_data()

        if self.date or self.author:
            names = await self.find_picture(author=self.author, date=self.date)

            for name in names:
                await self.create_relationship_to_req(name)


async def main():
    agent = SearchingAgentByTitleAndAuthor("bolt://localhost:7687", "DANIIL", "12345678", "vlad1")
    await agent.add_picture_to_req_context()


if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(main())
