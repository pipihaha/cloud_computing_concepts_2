/**********************************
 * FILE NAME: MP2Node.h
 *
 * DESCRIPTION: MP2Node class header file
 **********************************/

#ifndef MP2NODE_H_
#define MP2NODE_H_

/**
 * Header files
 */
#include "stdincludes.h"
#include "EmulNet.h"
#include "Node.h"
#include "HashTable.h"
#include "Log.h"
#include "Params.h"
#include "Message.h"
#include "Queue.h"
#include "common.h"
#include <unordered_map>

typedef struct Quoram_Item {
	//int transID;
    MessageType type;
	int success_count;
	int fail_count;
	long timestamp;
    string key;
    string value;
}Quoram_Item;

/**
 * CLASS NAME: MP2Node
 *
 * DESCRIPTION: This class encapsulates all the key-value store functionality
 * 				including:
 * 				1) Ring
 * 				2) Stabilization Protocol
 * 				3) Server side CRUD APIs
 * 				4) Client side CRUD APIs
 */
class MP2Node {
private:
	// Vector holding the next two neighbors in the ring who have my replicas
	vector<Node> hasMyReplicas;
	// Vector holding the previous two neighbors in the ring whose replicas I have
	vector<Node> haveReplicasOf;
	// Ring
	vector<Node> ring;
	// Hash Table
	//HashTable * ht;
	unordered_map<string, pair<string, ReplicaType>> data_hashtable;
	// Member representing this member
	Member *memberNode;
	// Params object
	Params *par;
	// Object of EmulNet
	EmulNet * emulNet;
	// Object of Log
	Log * log;

    Node *cur_node;
	unordered_map<int, Quoram_Item> Quoram_Items;
	

public:
	MP2Node(Member *memberNode, Params *par, EmulNet *emulNet, Log *log, Address *addressOfMember);
	Member * getMemberNode() {
		return this->memberNode;
	}

	// ring functionalities
	void updateRing();
	vector<Node> getMembershipList();
	size_t hashFunction(string key);
	void findNeighbors();

	// client side CRUD APIs
	void clientCreate(string key, string value);
	void clientRead(string key);
	void clientUpdate(string key, string value);
	void clientDelete(string key);

	// receive messages from Emulnet
	bool recvLoop();
	static int enqueueWrapper(void *env, char *buff, int size);

	// handle messages from receiving queue
	void checkMessages();

	// coordinator dispatches messages to corresponding nodes
	void dispatchMessages(Message message);

	// find the addresses of nodes that are responsible for a key
	vector<Node> findNodes(string key);

	// server
    bool createKeyValue(string key, string value, ReplicaType replica, int transID);
    string readKey(string key, int transID);
    bool updateKeyValue(string key, string value, ReplicaType replica, int transID);
    bool deletekey(string key, int transID);

	// stabilization protocol - handle multiple failures
	void stabilizationProtocol();

    void insert_item_to_hashtable(MessageType type, string key, string value);
    void send_reply(int transID, bool success, Address* toaddr, string key, string value);
    void send_readreply(int transID, Address* toaddr, string key, string str);
	void share_load_to_node(Node& newnode, ReplicaType Reptype);
	void delete_load_from_node(Node& newnode);
    void handle_reply(Message *msg);
    void handle_readreply(Message *msg);

	~MP2Node();
};

#endif /* MP2NODE_H_ */
