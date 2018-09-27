package unit.kafka.controller

import kafka.controller.KafkaController
import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import kafka.zk.KafkaZkClient
import org.apache.kafka.server.policy.CreateTopicPolicy
import org.easymock.EasyMock
import org.junit.{Before, Test}
import org.junit.Assert.assertEquals
import org.scalatest.junit.JUnitSuite

class TopicsSatisfyMinRFTest extends JUnitSuite {
  private var createTopicPolicy : Option[CreateTopicPolicy] = null
  private var mockZkClient: KafkaZkClient = null
  @Before
  def setUp(): Unit = {
    val config = TestUtils.createBrokerConfigs(1, "")
      .map(prop => {
        prop.setProperty(KafkaConfig.DefaultReplicationFactorProp,"2")  // Set min RF to be 2
        prop.setProperty(KafkaConfig.CreateTopicPolicyClassNameProp, "kafka.server.LiCreateTopicPolicy")
        prop
      }).map(KafkaConfig.fromProps)
    createTopicPolicy = Option(config.head.getConfiguredInstance(KafkaConfig.CreateTopicPolicyClassNameProp,
      classOf[CreateTopicPolicy]))
    mockZkClient = EasyMock.createMock(classOf[KafkaZkClient])
  }

  // Mock new topic with sufficient RF, should satisfy
  @Test
  def testNewTopicSucceed: Unit = {
    val topicSucceed = "test_topic1"
    assertEquals(true, KafkaController.satisfiesLiCreateTopicPolicy(createTopicPolicy, mockZkClient,
      topicSucceed, Map(0 -> Seq(0, 1))))      // RF = 2
  }

  // Mock new topic with insufficient RF, should fail
  @Test
  def testNewTopicFail: Unit = {
    val topicFail = "test_topic1"
    EasyMock.expect(mockZkClient.getTopicPartitions(topicFail)).andReturn(Seq.empty)
    EasyMock.replay(mockZkClient)
    assertEquals(false, KafkaController.satisfiesLiCreateTopicPolicy(createTopicPolicy, mockZkClient,
      topicFail, Map(0 -> Seq(1))))            // RF = 1
    EasyMock.verify(mockZkClient)
  }

  // Mock existing topic with insufficient RF, because it's pre-existing, should still satisfy
  @Test
  def testExistingTopicSucceed: Unit = {
    val topicExisting = "test_topic1"
    EasyMock.expect(mockZkClient.getTopicPartitions(topicExisting)).andReturn(Seq("0"))  // Partition number = 1
    EasyMock.replay(mockZkClient)
    assertEquals(true, KafkaController.satisfiesLiCreateTopicPolicy(createTopicPolicy, mockZkClient,
      topicExisting, Map(0 -> Seq(1))))        // RF = 1
    EasyMock.verify(mockZkClient)
  }
}
