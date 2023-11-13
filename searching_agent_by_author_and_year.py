from neo4j import AsyncGraphDatabase
import asyncio


class SearchingAgentByTitleAndAuthor:
    URL = "bolt://localhost:7687"
    USERNAME = "DANIIL"
    PASSWORD = "12345678"
    DATABASE_NAME = "vlad1"

    result = None

    def __init__(self):
        self.driver = AsyncGraphDatabase.driver(
            self.URL, auth=(self.USERNAME, self.PASSWORD), database=self.DATABASE_NAME
        )
        self.author = ""
        self.date = ""

    async def get_initial_data(self):
        author_record = await self.driver.execute_query(
            "MATCH (n: Node)<-[:nrel_author]-(m: Class {name: 'concept_request'}) RETURN n.name"
        )
        if len(author_record.records):
            self.author = author_record.records[0]["n.name"]
        print(f"check 2 {author_record}")

        date_record = await self.driver.execute_query(
            "MATCH (n: Node)<-[:nrel_date]-(m: Class {name: 'concept_request'}) RETURN n.name"
        )
        print(f"check 2 {date_record}")
        if len(date_record.records):
            self.date = date_record.records[0]["n.name"]

    def form_list_names(self, records):
        list_names = []
        for record in records:
            list_names.append(record["ns"]._properties["name"])

        return list_names

    async def find_picture_by_author(self, author):
        record = await self.driver.execute_query(
            "MATCH (n: Node {name: "
            + f"'{author}'"
            + "})-[:nrel_write]->(ns: Node) RETURN ns"
        )
        print(f"check {record}")
        return self.form_list_names(record.records)

    async def find_picture_by_date(self, date):
        record = await self.driver.execute_query(
            "MATCH (n: Node)-[:nrel_date]->(ns: Node {name: " + f"'{date}'" + "}) RETURN n"
        )
        return self.form_list_names(record.records)

    async def find_picture_by_author_and_date(self, author, date):
        print("dadsadsa")
        record = await self.driver.execute_query(
            "MATCH (n: Node {name: "
            + f"'{author}'"
            + "})-[:nrel_write]->(ns: Node)-[:nrel_date]->(d: Node {name: '" + date + "'}) RETURN ns"
        )
        records = record.records
        print(records)
        return self.form_list_names(records)

    async def find_picture(self, author="", date=""):
        if author and not date:
            names = await self.find_picture_by_author(author)
        elif date and author:
            names = await self.find_picture_by_author_and_date(author, date)
        else:
            names = await self.find_picture_by_date(date)

        return names

    async def create_relationship_to_req(self, name):
        await self.driver.execute_query(
            "MATCH (p: Node {name: '"
            + name
            + "'}), (req: Class {name: 'concept_request'}) CREATE (req)-[:nrel_context]->(p)"
        )

    async def add_picture_to_req_context(self):
        await self.get_initial_data()

        if self.date or self.author:
            names = await self.find_picture(author=self.author, date=self.date)

            for name in names:
                await self.create_relationship_to_req(name)


async def main():
    agent = SearchingAgentByTitleAndAuthor()
    await agent.add_picture_to_req_context()


if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(main())
