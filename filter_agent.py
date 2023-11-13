import asyncio
from neo4j import AsyncGraphDatabase


class PictureFilterAgent:
    def __init__(self, uri, username, password):
        self.driver = AsyncGraphDatabase.driver(uri, auth=(username, password))

    async def execute_query(self, query, parameters=None):
        async with self.driver.session() as session:
            result = await session.run(query, parameters=parameters)
            return result

    async def filter_pictures_by_author_or_name(self, author=None, name=None):
        if author and name:
            query = "MATCH (authorNode: Node {name: $author})-[:nrel_write]->(pictureNode: Node {name: $name}) RETURN pictureNode"
            parameters = {"author": author, "name": name}
        elif author:
            query = "MATCH (authorNode: Node {name: $author})-[:nrel_write]->(pictureNode: Node) RETURN pictureNode"
            parameters = {"author": author}
        elif name:
            query = "MATCH (pictureNode: Node {name: $name}) RETURN pictureNode"
            parameters = {"name": name}
        else:
            return []

        result = await self.execute_query(query, parameters)
        return [record["pictureNode"]["name"] for record in result]

    async def filter_pictures_by_context(self, request_name, context_author=None, context_name=None):
        if context_author or context_name:
            filtered_names = await self.filter_pictures_by_author_or_name(context_author, context_name)
            query = (
                "MATCH (requestNode: Class {name: $request_name}), (pictureNode: Node) "
                "WHERE pictureNode.name IN $filtered_names "
                "CREATE (requestNode)-[:nrel_found_picture]->(pictureNode)"
            )
            parameters = {"request_name": request_name, "filtered_names": filtered_names}
            await self.execute_query(query, parameters)


async def main():
    uri = "bolt://localhost:7687"
    username = "DANIIL"
    password = "12345678"

    filter_agent = PictureFilterAgent(uri, username, password)

    await filter_agent.filter_pictures_by_context("concept_request", context_author="Leonardo da Vinci",
                                                  context_name="Mona Lisa")


if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(main())
