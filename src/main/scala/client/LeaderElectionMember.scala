package client


trait LeaderElectionMember {
  def hasLeadership: Boolean
}
