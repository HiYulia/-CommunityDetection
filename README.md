# CommunityDetection

Implement Girvan-Newman algorithm to detect the communities in the network graph. 

step1:calculate the betweenness of each edge in the original graph.
(Betweenness: Number of shortest paths passing over the edge)

step2:calculate modularity, remove edges from the edge with the highest betweeness score, re-compute the betweenness and modularity.

step3: repeat step2 until reaches the global highest modularity.
