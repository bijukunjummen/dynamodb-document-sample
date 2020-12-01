package sample.dyn.repo

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.assertj.core.api.Assertions.assertThat
import org.bk.aws.dynamo.util.JsonAttributeValueUtil
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.RegisterExtension
import org.skyscreamer.jsonassert.JSONAssert
import reactor.core.publisher.Mono
import reactor.test.StepVerifier
import sample.dyn.migrator.DynamoMigrator
import sample.dyn.migrator.TableDefinition
import sample.dyn.model.Hotel
import sample.dyn.rules.TestContainerDynamoDBExtension
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndex
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement
import software.amazon.awssdk.services.dynamodb.model.KeyType
import software.amazon.awssdk.services.dynamodb.model.Projection
import software.amazon.awssdk.services.dynamodb.model.ProjectionType
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType

class HotelDocumentTest {

    private var objectMapper: ObjectMapper = jacksonObjectMapper()

    @Test
    fun saveTopLevel() {
        val hotel = """
            {
                "id": "1",
                "name": "test",
                "address": "test address",
                "state": "OR",
                "properties": {
                    "amenities":{
                        "rooms": 100,
                        "gym": 2,
                        "swimmingPool": true                    
                    }                    
                },
                "zip": "zip"
            }
        """.trimIndent()
        val attributeValue = JsonAttributeValueUtil.toAttributeValue(hotel, objectMapper)
        StepVerifier
            .create(
                Mono.fromCompletionStage {
                    dynamoExtension.asyncClient.putItem(
                        PutItemRequest.builder()
                            .tableName(DynamoHotelRepo.TABLE_NAME)
                            .item(attributeValue.m())
                            .build()
                    )
                }
            )
            .assertNext { response ->
                assertThat(response.attributes()).isEmpty()
            }
            .verifyComplete()

        StepVerifier
            .create(
                Mono.fromCompletionStage {
                    dynamoExtension.asyncClient.getItem(
                        GetItemRequest.builder()
                            .tableName(DynamoHotelRepo.TABLE_NAME)
                            .key(mapOf("id" to AttributeValue.builder().s("1").build()))
                            .build()
                    )
                }
            )
            .assertNext { response ->
                assertThat(response.item()).isNotEmpty()
                val jsonNode =
                    JsonAttributeValueUtil.fromAttributeValue(response.item())
                JSONAssert.assertEquals(objectMapper.writeValueAsString(jsonNode), hotel, true)
                val hotelResponse = objectMapper.treeToValue(jsonNode, Hotel::class.java)
                assertThat(hotelResponse.address).isEqualTo("test address")
            }
            .verifyComplete()
    }

    companion object {
        @RegisterExtension
        @JvmField
        val dynamoExtension = TestContainerDynamoDBExtension()

        @BeforeAll
        @JvmStatic
        fun beforeAll() {
            val migrator = DynamoMigrator(dynamoExtension.syncClient)
            migrator
                .migrate(listOf(hotelTableDefinition()))
                .blockLast()
        }

        fun hotelTableDefinition(): TableDefinition {
            val byStateIndex: GlobalSecondaryIndex = GlobalSecondaryIndex.builder()
                .indexName(DynamoHotelRepo.HOTELS_BY_STATE_INDEX)
                .keySchema(
                    KeySchemaElement.builder()
                        .attributeName(DynamoHotelRepo.STATE)
                        .keyType(KeyType.HASH).build(),
                    KeySchemaElement.builder()
                        .attributeName(DynamoHotelRepo.NAME)
                        .keyType(KeyType.RANGE).build()
                )
                .provisionedThroughput(
                    ProvisionedThroughput.builder()
                        .readCapacityUnits(10)
                        .writeCapacityUnits(10)
                        .build()
                )
                .projection(Projection.builder().projectionType(ProjectionType.ALL).build())
                .build()

            return TableDefinition(
                tableName = DynamoHotelRepo.TABLE_NAME,
                attributeDefinitions = listOf(
                    AttributeDefinition.builder()
                        .attributeName(DynamoHotelRepo.ID)
                        .attributeType(ScalarAttributeType.S).build(),
                    AttributeDefinition.builder()
                        .attributeName(DynamoHotelRepo.NAME)
                        .attributeType(ScalarAttributeType.S).build(),
                    AttributeDefinition.builder()
                        .attributeName(DynamoHotelRepo.STATE)
                        .attributeType(ScalarAttributeType.S).build()
                ),
                keySchemaElements = listOf(
                    KeySchemaElement.builder()
                        .attributeName(DynamoHotelRepo.ID)
                        .keyType(KeyType.HASH).build()
                ),
                globalSecondaryIndex = listOf(byStateIndex),
                localSecondaryIndex = emptyList(),
                provisionedThroughput = ProvisionedThroughput.builder()
                    .readCapacityUnits(10)
                    .writeCapacityUnits(10)
                    .build()

            )
        }

    }
}