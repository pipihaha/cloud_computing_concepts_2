/**********************************
 * FILE NAME: MP2Node.cpp
 *
 * DESCRIPTION: MP2Node class definition
 **********************************/
#include "MP2Node.h"
#define DEBUGLOG_MP2_1 1
/**
 * constructor
 */
MP2Node::MP2Node(Member *memberNode, Params *par, EmulNet * emulNet, Log * log, Address * address) {
#ifdef DEBUGLOG_MP2
    log->LOG(&memberNode->addr, "Enter MP2Node");
#endif
	this->memberNode = memberNode;
	this->par = par;
	this->emulNet = emulNet;
	this->log = log;
	//ht = new HashTable();
	this->memberNode->addr = *address;

    //Address cur_addr = *address;
    //cur_addr.addr[5] = '1';
    this->cur_node = new Node(*address);

#ifdef DEBUGLOG_MP2
    log->LOG(&memberNode->addr, "Leaving MP2Node");
#endif
	//this->transID = 0;
}

/**
 * Destructor
 */
MP2Node::~MP2Node() {
	//delete ht;
	delete memberNode;
    delete cur_node;
}

/**
 * FUNCTION NAME: updateRing
 *
 * DESCRIPTION: This function does the following:
 * 				1) Gets the current membership list from the Membership Protocol (MP1Node)
 * 				   The membership list is returned as a vector of Nodes. See Node class in Node.h
 * 				2) Constructs the ring based on the membership list
 * 				3) Calls the Stabilization Protocol
 */
void MP2Node::updateRing() {
	/*
	 * Implement this. Parts of it are already implemented
	 */
	vector<Node> curMemList;
	bool change = false;

    if (memberNode->bFailed) {
        return;
    }
	/*
	 *  Step 1. Get the current membership list from Membership Protocol / MP1
	 */
	curMemList = getMembershipList();

	/*
	 * Step 2: Construct the ring
	 */
	curMemList.push_back(*cur_node);

	// Sort the list based on the hashCode
    auto compare = [](Node& node1, Node& node2) {return node1.nodeHashCode < node2.nodeHashCode;};

    sort(curMemList.begin(), curMemList.end(), compare);
	ring = curMemList;

	/*
	 * Step 3: Run the stabilization protocol IF REQUIRED
	 */
	// Run stabilization protocol if the hash table size is greater than zero and if there has been a changed in the ring
	stabilizationProtocol();

	return;
}

/**
 * FUNCTION NAME: getMemberhipList
 *
 * DESCRIPTION: This function goes through the membership list from the Membership protocol/MP1 and
 * 				i) generates the hash code for each member
 * 				ii) populates the ring member in MP2Node class
 * 				It returns a vector of Nodes. Each element in the vector contain the following fields:
 * 				a) Address of the node
 * 				b) Hash code obtained by consistent hashing of the Address
 */
vector<Node> MP2Node::getMembershipList() {
	unsigned int i;
	vector<Node> curMemList;
	for ( i = 0 ; i < this->memberNode->memberList.size(); i++ ) {
		Address addressOfThisMember;
		int id = this->memberNode->memberList.at(i).getid();
        short port = this->memberNode->memberList.at(i).getport();
		memcpy(&addressOfThisMember.addr[0], &id, sizeof(int));
		memcpy(&addressOfThisMember.addr[4], &port, sizeof(short));
		curMemList.emplace_back(Node(addressOfThisMember));
	}
	return curMemList;
}

/**
 * FUNCTION NAME: hashFunction
 *
 * DESCRIPTION: This functions hashes the key and returns the position on the ring
 * 				HASH FUNCTION USED FOR CONSISTENT HASHING
 *
 * RETURNS:
 * size_t position on the ring
 */
size_t MP2Node::hashFunction(string key) {
	std::hash<string> hashFunc;
	size_t ret = hashFunc(key);
	return ret%RING_SIZE;
}

void MP2Node::insert_item_to_hashtable(MessageType type, string key, string value) {
	Quoram_Item item;
	item.type = type;
    item.transID = g_transID;
	item.success_count = 0;
	item.fail_count = 0;
	item.timestamp = memberNode->heartbeat;
    item.key = key;
    item.value = value;
	Quoram_Items[g_transID] = item;

	return;
}

/**
 * FUNCTION NAME: clientCreate
 *
 * DESCRIPTION: client side CREATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientCreate(string key, string value) {
	/*
	 * Implement this
	 */
    Address mp2_address = cur_node->nodeAddress;
    mp2_address.addr[5] = '1';

	vector<Node> nodes = findNodes(key);
	g_transID += 1;

    insert_item_to_hashtable(CREATE, key, value);

	Message msg(g_transID, memberNode->addr, CREATE, key, value, PRIMARY);
	string msg_str = msg.toString();

	for (auto target_node : nodes) {
        Address target_address = target_node.nodeAddress;
        target_address.addr[5] = '1';
        emulNet->ENsend(&mp2_address, &target_address, msg_str);
        if (msg.replica == PRIMARY) {
            msg.replica = SECONDARY;
        }
        else if (msg.replica == SECONDARY) {
            msg.replica = TERTIARY;
        }
		msg_str = msg.toString();
    }

	return;
}

/**
 * FUNCTION NAME: clientRead
 *
 * DESCRIPTION: client side READ API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientRead(string key){
	/*
	 * Implement this
	 */
    Address mp2_address = cur_node->nodeAddress;
    mp2_address.addr[5] = '1';

	vector<Node> nodes = findNodes(key);
	g_transID += 1;

    insert_item_to_hashtable(READ, key, "");

	Message msg(g_transID, memberNode->addr, READ, key);
	string msg_str = msg.toString();

	for (auto target_node : nodes) {
        Address target_address = target_node.nodeAddress;
        target_address.addr[5] = '1';
        emulNet->ENsend(&mp2_address, &target_address, msg_str);
	}

	return;
}

/**
 * FUNCTION NAME: clientUpdate
 *
 * DESCRIPTION: client side UPDATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientUpdate(string key, string value){
	/*
	 * Implement this
	 */
    Address mp2_address = cur_node->nodeAddress;
    mp2_address.addr[5] = '1';

	vector<Node> nodes = findNodes(key);
	g_transID += 1;

    insert_item_to_hashtable(UPDATE, key, value);

	Message msg(g_transID, memberNode->addr, UPDATE, key, value, PRIMARY);
	string msg_str = msg.toString();

	for (auto target_node : nodes) {
        Address target_address = target_node.nodeAddress;
        target_address.addr[5] = '1';
        emulNet->ENsend(&mp2_address, &target_address, msg_str);
        if (msg.replica == PRIMARY) {
            msg.replica = SECONDARY;
        }
        else if (msg.replica == SECONDARY) {
            msg.replica = TERTIARY;
        }
		msg_str = msg.toString();
	}

	return;
}

/**
 * FUNCTION NAME: clientDelete
 *
 * DESCRIPTION: client side DELETE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientDelete(string key){
	/*
	 * Implement this
	 */
    Address mp2_address = cur_node->nodeAddress;
    mp2_address.addr[5] = '1';
	vector<Node> nodes = findNodes(key);
	g_transID += 1;

    insert_item_to_hashtable(DELETE, key, "");

	Message msg(g_transID, memberNode->addr, DELETE, key);
	string msg_str = msg.toString();

	for (auto target_node : nodes) {
        Address target_address = target_node.nodeAddress;
        target_address.addr[5] = '1';
        emulNet->ENsend(&mp2_address, &target_address, msg_str);
        if (msg.replica == PRIMARY) {
            msg.replica = SECONDARY;
        }
        else if (msg.replica == SECONDARY) {
            msg.replica = TERTIARY;
        }
		msg_str = msg.toString();
	}

	return;
}

/**
 * FUNCTION NAME: createKeyValue
 *
 * DESCRIPTION: Server side CREATE API
 * 			   	The function does the following:
 * 			   	1) Inserts key value into the local hash table
 * 			   	2) Return true or false based on success or failure
 */
bool MP2Node::createKeyValue(string key, string value, ReplicaType replica, int transID) {
	/*
	 * Implement this
	 */
	// Insert key, value, replicaType into the hash table
	//vector<Node> nodes = findNodes(key);
	
	data_hashtable[key] = make_pair(value, replica);
	
    log->logCreateSuccess(&memberNode->addr, false, transID, key, value);

	return true;
}

/**
 * FUNCTION NAME: readKey
 *
 * DESCRIPTION: Server side READ API
 * 			    This function does the following:
 * 			    1) Read key from local hash table
 * 			    2) Return value
 */
string MP2Node::readKey(string key, int transID) {
	/*
	 * Implement this
	 */
	// Read key from local hash table and return value
	string str = (data_hashtable.count(key) == 0) ? "" : data_hashtable[key].first;

	if (!str.compare("")) {
        log->logReadFail(&memberNode->addr, false, transID, key);
	}
	else {
        log->logReadSuccess(&memberNode->addr, false, transID, key, str);
	}

	return str;
}

/**
 * FUNCTION NAME: updateKeyValue
 *
 * DESCRIPTION: Server side UPDATE API
 * 				This function does the following:
 * 				1) Update the key to the new value in the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::updateKeyValue(string key, string value, ReplicaType replica, int transID) {
	/*
	 * Implement this
	 */
	// Update key in local hash table and return true or false
	bool res;

	if (data_hashtable.count(key) == 0) {
		res = false;
	}
	else {
		res = true;
		data_hashtable[key] = make_pair(value, replica);
	}

	if (!res) {
		log->logUpdateFail(&memberNode->addr, false, transID, key, value);
	}
	else {
		log->logUpdateSuccess(&memberNode->addr, false, transID, key, value);
	}

	return res;
}

/**
 * FUNCTION NAME: deleteKey
 *
 * DESCRIPTION: Server side DELETE API
 * 				This function does the following:
 * 				1) Delete the key from the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::deletekey(string key, int transID) {
	/*
	 * Implement this
	 */
	// Delete the key from the local hash table

	bool res;

	if (data_hashtable.count(key) == 0) {
		res = false;
	}
	else {
		res = true;
		data_hashtable.erase(key);
	}

	if (!res) {
		log->logDeleteFail(&memberNode->addr, false, transID, key);
	}
	else {
		log->logDeleteSuccess(&memberNode->addr, false, transID, key);
	}

	return res;
}

void MP2Node::send_reply(int transID, bool success, Address* toaddr, string key, string value) {

	Message msg(transID, memberNode->addr, REPLY, success);
    msg.key = key;
    msg.value = value;
	string msg_str = msg.toString();

    Address mp2_address = cur_node->nodeAddress;
    mp2_address.addr[5] = '1';

    emulNet->ENsend(&mp2_address, toaddr, msg_str);

	return;
}

void MP2Node::send_readreply(int transID, Address* toaddr, string key, string str) {

	Message msg(transID, memberNode->addr, str);
    msg.key = key;
	string msg_str = msg.toString();
	
    Address mp2_address = cur_node->nodeAddress;
    mp2_address.addr[5] = '1';

    emulNet->ENsend(&mp2_address, toaddr, msg_str);

	return;
}


void MP2Node::handle_reply(Message *msg) {

	int transID = msg->transID;

	if (Quoram_Items.count(transID) == 0) {
		return;
	} 

	if (msg->success) {
		Quoram_Items[transID].success_count += 1;
	}
	else {
		Quoram_Items[transID].fail_count += 1;
	}

    switch (Quoram_Items[transID].type) {
	    case CREATE:
			if (Quoram_Items[transID].success_count > 1) {
                log->logCreateSuccess(&memberNode->addr, true, transID, Quoram_Items[transID].key, Quoram_Items[transID].value);
			}
			else if (Quoram_Items[transID].fail_count > 1) {
                log->logCreateFail(&memberNode->addr, true, transID, Quoram_Items[transID].key, Quoram_Items[transID].value);
			}
            break;
		case UPDATE:
			if (Quoram_Items[transID].success_count > 1) {
                log->logUpdateSuccess(&memberNode->addr, true, transID, Quoram_Items[transID].key, Quoram_Items[transID].value);
			}
			else if (Quoram_Items[transID].fail_count > 1) {
                log->logUpdateFail(&memberNode->addr, true, transID, Quoram_Items[transID].key, Quoram_Items[transID].value);
			}
            break;
		case DELETE:
			if (Quoram_Items[transID].success_count > 1) {
                log->logDeleteSuccess(&memberNode->addr, true, transID, Quoram_Items[transID].key);
			}
			else if (Quoram_Items[transID].fail_count > 1) {
                log->logDeleteFail(&memberNode->addr, true, transID, Quoram_Items[transID].key);
			}
            break;
		default:
            break;
			//ASSERT(0);
	}

	if (Quoram_Items[transID].success_count > 1 || Quoram_Items[transID].fail_count > 1) {
		Quoram_Items.erase(transID);
	}

	return;
}

void MP2Node::handle_readreply(Message *msg) {

	int transID = msg->transID;

	if (Quoram_Items.count(transID) == 0) {
		return;
	}

	if (msg->value.compare("")) {
		Quoram_Items[transID].success_count += 1;
	}
	else {
		Quoram_Items[transID].fail_count += 1;
	}
	
	if (Quoram_Items[transID].success_count > 1) {
        log->logReadSuccess(&memberNode->addr, true, transID, Quoram_Items[transID].key, msg->value);
		Quoram_Items.erase(transID);
	}
	else if (Quoram_Items[transID].fail_count > 1){
        log->logReadFail(&memberNode->addr, true, transID, Quoram_Items[transID].key);
		Quoram_Items.erase(transID);
	}

	return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: This function is the message handler of this node.
 * 				This function does the following:
 * 				1) Pops messages from the queue
 * 				2) Handles the messages according to message types
 */
void MP2Node::checkMessages() {
	/*
	 * Implement this. Parts of it are already implemented
	 */
	char * data;
	int size;

	/*
	 * Declare your local variables here
	 */

	bool success;
	string read_str;

    if (memberNode->bFailed) {
        return;
    }

	// dequeue all messages and handle them
	while ( !memberNode->mp2q.empty() ) {
		/*
		 * Pop a message from the queue
		 */
		data = (char *)memberNode->mp2q.front().elt;
		size = memberNode->mp2q.front().size;
		memberNode->mp2q.pop();

		string str(data, data + size);
		Message msg(str);

		//CREATE, READ, UPDATE, DELETE, REPLY, READREPLY
		switch (msg.type) {
		    case CREATE:
                success = createKeyValue(msg.key, msg.value, msg.replica, msg.transID);
                send_reply(msg.transID, success, &msg.fromAddr, msg.key, msg.value);
				break;
			case READ:
                read_str = readKey(msg.key, msg.transID);
                send_readreply(msg.transID, &msg.fromAddr, msg.key, read_str);
				break;
			case UPDATE:
                success = updateKeyValue(msg.key, msg.value, msg.replica, msg.transID);
                send_reply(msg.transID, success, &msg.fromAddr, msg.key, msg.value);
                
				break;
			case DELETE:
                success = deletekey(msg.key, msg.transID);
                send_reply(msg.transID, success, &msg.fromAddr, msg.key, msg.value);
				break;
			case REPLY:
				handle_reply(&msg);
				break;
			case READREPLY:
				handle_readreply(&msg);
				break;
			default: 
                break;
				//ASSERT(0);
		}
	}

	/*
	 * This function should also ensure all READ and UPDATE operation
	 * get QUORUM replies
	 */

}

/**
 * FUNCTION NAME: findNodes
 *
 * DESCRIPTION: Find the replicas of the given keyfunction
 * 				This function is responsible for finding the replicas of a key
 */
vector<Node> MP2Node::findNodes(string key) {
	size_t pos = hashFunction(key);
	vector<Node> addr_vec;
	if (ring.size() >= 3) {
		// if pos <= min || pos > max, the leader is the min
		if (pos <= ring.at(0).getHashCode() || pos > ring.at(ring.size()-1).getHashCode()) {
			addr_vec.emplace_back(ring.at(0));
			addr_vec.emplace_back(ring.at(1));
			addr_vec.emplace_back(ring.at(2));
		}
		else {
			// go through the ring until pos <= node
			for (int i=1; i<ring.size(); i++){
				Node addr = ring.at(i);
				if (pos <= addr.getHashCode()) {
					addr_vec.emplace_back(addr);
					addr_vec.emplace_back(ring.at((i+1)%ring.size()));
					addr_vec.emplace_back(ring.at((i+2)%ring.size()));
					break;
				}
			}
		}
	}

	return addr_vec;
}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: Receive messages from EmulNet and push into the queue (mp2q)
 */
bool MP2Node::recvLoop() {

    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
        return emulNet->ENrecv(&(cur_node->nodeAddress), this->enqueueWrapper, NULL, 1, &(memberNode->mp2q));
    }
#ifdef DEBUGLOG_MP2
    
#endif
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue of MP2Node
 */
int MP2Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}


void MP2Node::share_load_to_node(Node& newnode, ReplicaType Reptype) {

	int transID = 0;
	string key, value, msg_str;
	ReplicaType type;

	Message msg(transID, memberNode->addr, CREATE, key, value, Reptype);

	for (auto entry : data_hashtable) {
		key = entry.first;
		value = entry.second.first;
		type = entry.second.second;

		if (type == PRIMARY) {
			msg.key = key;
			msg.value = value;

			msg_str = msg.toString();
			emulNet->ENsend(&memberNode->addr, &newnode.nodeAddress, (char *)&msg_str, msg_str.size());
		}

	}

	return;
}

void MP2Node::delete_load_from_node(Node& oldnode) {
	int transID = 0;
	string key, msg_str;
	ReplicaType type;

	Message msg(transID, memberNode->addr, DELETE, key);

	for (auto entry : data_hashtable) {
		key = entry.first;
		type = entry.second.second;

		if (type == PRIMARY) {
			msg.key = key;
			msg_str = msg.toString();
			emulNet->ENsend(&memberNode->addr, &oldnode.nodeAddress, (char *)&msg_str, msg_str.size());
		}

	}

	return;
}

/**
 * FUNCTION NAME: stabilizationProtocol
 *
 * DESCRIPTION: This runs the stabilization protocol in case of Node joins and leaves
 * 				It ensures that there always 3 copies of all keys in the DHT at all times
 * 				The function does the following:
 *				1) Ensures that there are three "CORRECT" replicas of all the keys in spite of failures and joins
 *				Note:- "CORRECT" replicas implies that every key is replicated in its two neighboring nodes in the ring
 */
void MP2Node::stabilizationProtocol() {
	/*
	 * Implement this
	 */
    if (ring.size() == 0) {
        return;
    }

    if (memberNode->bFailed) {
        return;
    }
	// find index for current node
	int index;
	for (index = 0; index < ring.size(); index ++) {
        if (ring[index].nodeAddress == cur_node->nodeAddress) {
			break;
		}
	}

	vector<Node> new_hasMyReplicas;

    index = (index + 1) % ring.size();
    new_hasMyReplicas.push_back(ring[index]);

    index = (index + 1) % ring.size();
    new_hasMyReplicas.push_back(ring[index]);

	if (hasMyReplicas.size() == 0) {
		hasMyReplicas = new_hasMyReplicas;
		share_load_to_node(hasMyReplicas[0], SECONDARY);
		share_load_to_node(hasMyReplicas[1], TERTIARY);

		return;
	}

	//create replicas on new nodes
	for (index = 0; index < 2; index++) {
        if (new_hasMyReplicas[index].nodeHashCode != hasMyReplicas[0].nodeHashCode && new_hasMyReplicas[index].nodeHashCode != hasMyReplicas[1].nodeHashCode) { // new node added in, share some load on it.
			ReplicaType type = (index == 0) ? SECONDARY : TERTIARY;
			share_load_to_node(new_hasMyReplicas[index], type);
		}
	}
	
	//delete key/value from some nodes
	for (index = 0; index < 2; index++) {
        if (hasMyReplicas[index].nodeHashCode != new_hasMyReplicas[0].nodeHashCode && hasMyReplicas[index].nodeHashCode != new_hasMyReplicas[1].nodeHashCode) { // new node added in, share some load on it.
			delete_load_from_node(hasMyReplicas[index]);
		}
	}

    auto iter = Quoram_Items.begin();
    while (iter != Quoram_Items.end()) {
        Quoram_Item item = (*iter).second;
        if (memberNode->heartbeat - item.timestamp > 5) {
            switch (item.type) {
            case READ:
                log->logReadFail(&memberNode->addr, true, item.transID, item.key);
                break;
            case CREATE:
                log->logCreateFail(&memberNode->addr, true, item.transID, item.key, item.value);
                break;
            case UPDATE:
                log->logUpdateFail(&memberNode->addr, true, item.transID, item.key, item.value);
                break;
            case DELETE:
                log->logDeleteFail(&memberNode->addr, true, item.transID, item.key);
                break;
            default:
                break;
            }

            iter = Quoram_Items.erase(iter);
        }
        else {
            iter++;
        }
    }


	hasMyReplicas = new_hasMyReplicas;

	return;
}
