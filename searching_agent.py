import asyncio

from neo4j import AsyncGraphDatabase


class PictureSearchAgent:
    def __init__(self, uri, username, password):
        self.driver = AsyncGraphDatabase.driver(uri, auth=(username, password))

    async def execute_query(self, query, parameters=None):
        async with self.driver.session() as session:
            result = await session.run(query, parameters=parameters)
            return result

    async def find_pictures_by_author(self, author):
        query = "MATCH (authorNode: Node {name: $author})-[:nrel_write]->(pictureNode: Node) RETURN pictureNode"
        result = await self.execute_query(query, parameters={"author": author})
        return [record["pictureNode"]["name"] for record in result]

    async def find_pictures_by_year(self, year):
        query = "MATCH (pictureNode: Node {year: $year}) RETURN pictureNode"
        result = await self.execute_query(query, parameters={"year": year})
        return [record["pictureNode"]["name"] for record in result]

    async def find_pictures_by_author_and_year(self, author, year):
        query = "MATCH (authorNode: Node {name: $author})-[:nrel_write]->(pictureNode: Node {year: $year}) RETURN pictureNode"
        result = await self.execute_query(query, parameters={"author": author, "year": year})
        return [record["pictureNode"]["name"] for record in result]

    async def add_pictures_to_request_context(self, author=None, year=None):
        if author and not year:
            names = await self.find_pictures_by_author(author)
        elif year and author:
            names = await self.find_pictures_by_author_and_year(author, year)
        else:
            names = await self.find_pictures_by_year(year)

        async with self.driver.session() as session:
            request_node_query = "CREATE (requestNode: Class {name: 'concept_request'})"
            await session.write_transaction(lambda tx: tx.run(request_node_query))

            for name in names:
                request_relationship_query = (
                    "MATCH (pictureNode: Node {name: $name}), (requestNode: Class {name: 'concept_request'}) "
                    "CREATE (requestNode)-[:nrel_found_picture]->(pictureNode)"
                )
                await session.write_transaction(lambda tx: tx.run(request_relationship_query, name=name))


async def main():
    uri = "bolt://localhost:7687"
    username = "DANIIL"
    password = "12345678"
    agent = PictureSearchAgent(uri, username, password)
    await agent.add_pictures_to_request_context(author="Leonardo da Vinci", year="1503")


if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(main())
