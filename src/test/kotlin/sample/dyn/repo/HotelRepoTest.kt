package sample.dyn.repo

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.RegisterExtension
import reactor.test.StepVerifier
import sample.dyn.migrator.DynamoMigrator
import sample.dyn.migrator.TableDefinition
import sample.dyn.model.Hotel
import sample.dyn.rules.TestContainerDynamoDBExtension
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndex
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement
import software.amazon.awssdk.services.dynamodb.model.KeyType
import software.amazon.awssdk.services.dynamodb.model.Projection
import software.amazon.awssdk.services.dynamodb.model.ProjectionType
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType

class HotelRepoTest {

    private val objectMapper = jacksonObjectMapper()

    @Test
    fun saveHotel() {
        val hotelRepo = DynamoHotelRepo(dynamoExtension.asyncClient);
        val hotel = Hotel(
            id = "1",
            name = "test hotel",
            address = "test address",
            state = "OR",
            zip = "zip",
            properties = objectMapper.createObjectNode().put("test", "test")
        )
        val resp = hotelRepo.saveHotel(hotel)

        StepVerifier.create(resp)
            .expectNext(hotel)
            .expectComplete()
            .verify()
    }

    @Test
    fun updateHotel() {
        val hotelRepo = DynamoHotelRepo(dynamoExtension.asyncClient);
        val hotel = Hotel(
            id = "1",
            name = "test hotel",
            address = "test address",
            state = "OR",
            zip = "zip",
            properties = objectMapper.createObjectNode().put("test", "test")
        )
        val resp = hotelRepo.saveHotel(hotel)

        StepVerifier.create(resp.flatMap { savedHotel -> hotelRepo.updateHotel(savedHotel) })
            .expectNext(hotel.copy(version = hotel.version + 1))
            .expectComplete()
            .verify()
    }

    @Test
    fun deleteHotel() {
        val hotelRepo = DynamoHotelRepo(dynamoExtension.asyncClient);
        val hotel = Hotel(id = "1", name = "test hotel", address = "test address", state = "OR", zip = "zip")
        val deleteResp = hotelRepo
            .saveHotel(hotel)
            .flatMap { hotelRepo.deleteHotel("1") }

        StepVerifier.create(deleteResp)
            .expectNext(true)
            .expectComplete()
            .verify()
    }

    @Test
    fun deleteNonExistentHotel() {
        val hotelRepo = DynamoHotelRepo(dynamoExtension.asyncClient);
        val deleteResp = hotelRepo.deleteHotel("1")

        StepVerifier.create(deleteResp)
            .expectNext(true)
            .expectComplete()
            .verify()
    }

    @Test
    fun findHotelsByState() {
        val hotelRepo = DynamoHotelRepo(dynamoExtension.asyncClient);
        val hotel1 = Hotel(id = "1", name = "test hotel1", address = "test address1", state = "OR", zip = "zip")
        val hotel2 = Hotel(id = "2", name = "test hotel2", address = "test address2", state = "OR", zip = "zip")
        val hotel3 = Hotel(id = "3", name = "test hotel3", address = "test address3", state = "WA", zip = "zip")
        val resp = hotelRepo.saveHotel(hotel1)
            .then(hotelRepo.saveHotel(hotel2))
            .then(hotelRepo.saveHotel(hotel3))

        StepVerifier.create(resp)
            .expectNext(hotel3)
            .expectComplete()
            .verify()

        StepVerifier.create(hotelRepo.findHotelsByState("OR"))
            .expectNext(hotel1, hotel2)
            .expectComplete()
            .verify()

        StepVerifier.create(hotelRepo.findHotelsByState("WA"))
            .expectNext(hotel3)
            .expectComplete()
            .verify()

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
                .subscribe()
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