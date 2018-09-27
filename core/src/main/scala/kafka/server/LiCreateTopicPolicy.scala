package kafka.server

import org.apache.kafka.common.errors.PolicyViolationException
import org.apache.kafka.server.policy.CreateTopicPolicy

class LiCreateTopicPolicy extends CreateTopicPolicy {
  var minRf = 0

  @throws[PolicyViolationException]
  override def validate(requestMetadata: CreateTopicPolicy.RequestMetadata): Unit = {
    val requestTopic = requestMetadata.topic
    val requestRF = requestMetadata.replicationFactor()
    import collection.JavaConverters._
    val requestAssignment = requestMetadata.replicasAssignments().asScala

    if (requestAssignment == null && requestRF == null)
      throw new PolicyViolationException(s"Topic [$requestTopic] is missing both replica assignment and " +
        s"replication factor.")

    // In createTopics() in AdminManager, replicationFactor and replicasAssignments are not both set at same time. We
    // follow the same rationale here and prioritize replicasAssignments over replicationFactor
    if (requestAssignment != null) {
      requestAssignment.foreach { case (p, assignment) =>
          if (assignment.size() < minRf)
            throw new PolicyViolationException(s"Topic [$requestTopic] fails RF requirement. Received RF for " +
              s"[partition-$p]: ${assignment.size()}, min required RF: $minRf.")
      }
    } else if (requestRF < minRf) {
      throw new PolicyViolationException(s"Topic [$requestTopic] fails RF requirement. " +
        s"Received RF: ${requestMetadata.replicationFactor}, min required RF: $minRf.")
    }
  }

  /**
    * Configure this class with the given key-value pairs
    */
  override def configure(configs: java.util.Map[String, _]): Unit = {
    minRf = configs.get(KafkaConfig.DefaultReplicationFactorProp).asInstanceOf[String].toInt
  }

  @throws[Exception]
  override def close(): Unit = {
  }
}
