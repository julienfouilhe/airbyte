from unittest.mock import MagicMock
from destination_langchain.indexer import PineconeIndexer
from destination_langchain.config import PineconeIndexingModel
from langchain.document_loaders.base import Document
from unittest.mock import ANY
from airbyte_cdk.models import AirbyteStream, ConfiguredAirbyteCatalog, ConfiguredAirbyteStream


def create_pinecone_indexer():
    config = PineconeIndexingModel(mode="pinecone", pinecone_environment="myenv", pinecone_key="mykey", index="myindex")
    embedder = MagicMock()
    indexer = PineconeIndexer(config, embedder)

    indexer.pinecone_index.delete = MagicMock()
    indexer.embed_fn = MagicMock(return_value=[[1, 2, 3], [4, 5, 6]])
    indexer.pinecone_index.upsert = MagicMock()
    return indexer


def test_pinecone_index_upsert_and_delete():
    indexer = create_pinecone_indexer()
    indexer.index(
        [
            Document(page_content="test", metadata={"_airbyte_stream": "abc"}),
            Document(page_content="test2", metadata={"_airbyte_stream": "abc"}),
        ],
        ["delete_id1", "delete_id2"],
    )
    indexer.pinecone_index.delete.assert_called_with(filter={"_natural_id": {"$in": ["delete_id1", "delete_id2"]}})
    indexer.pinecone_index.upsert.assert_called_with(
        vectors=[
            (ANY, [1, 2, 3], {"_airbyte_stream": "abc", "text": "test"}),
            (ANY, [4, 5, 6], {"_airbyte_stream": "abc", "text": "test2"}),
        ]
    )

def test_pinecone_pre_sync():
    indexer = create_pinecone_indexer()
    indexer.pre_sync(ConfiguredAirbyteCatalog.parse_obj(
        {
            "streams": [
                {
                    "stream": {
                        "name": "example_stream",
                        "json_schema": {"$schema": "http://json-schema.org/draft-07/schema#", "type": "object", "properties": {}},
                        "supported_sync_modes": ["full_refresh", "incremental"],
                        "source_defined_cursor": False,
                        "default_cursor_field": ["column_name"],
                    },
                    "primary_key": [["id"]],
                    "sync_mode": "incremental",
                    "destination_sync_mode": "append_dedup",
                },
                {
                    "stream": {
                        "name": "example_stream2",
                        "json_schema": {"$schema": "http://json-schema.org/draft-07/schema#", "type": "object", "properties": {}},
                        "supported_sync_modes": ["full_refresh", "incremental"],
                        "source_defined_cursor": False,
                        "default_cursor_field": ["column_name"],
                    },
                    "primary_key": [["id"]],
                    "sync_mode": "full_refresh",
                    "destination_sync_mode": "overwrite",
                }
            ]
        }
    ))
    indexer.pinecone_index.delete.assert_called_with(filter={"_airbyte_stream": "example_stream2"})
