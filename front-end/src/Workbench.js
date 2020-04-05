import React from "react";

import Tabs from "react-bootstrap/Tabs";
import Tab from "react-bootstrap/Tab";

import SearchTab from "./SearchTab";
import Writeup from "./Writeup";
import TopicList from "./TopicList";

function initial_bench_state() {
  return {
	qrels: new Map(),
	writeup: {
	  title: "",
	  desc: "",
	  narr: "",
	},
	topics: [],
	cur_topic: -1
  };
}

class Workbench extends React.Component {
  constructor(props) {
	super(props);
	this.state = initial_bench_state();

	this.add_relevant = this.add_relevant.bind(this);
	this.remove_relevant = this.remove_relevant.bind(this);
	this.change_writeup = this.change_writeup.bind(this);
	this.save_topic = this.save_topic.bind(this);
	this.load_topic = this.load_topic.bind(this);
	this.delete_topic = this.delete_topic.bind(this);
  }

  clear_state() {
	this.setState(initial_bench_state());
  }

  /*
   * JSON.stringify serializes Maps as plain objects, so we need to handle their
   * conversion in stringify and parse so when we restore them, we get maps.
   */
  JSON_stringify_maps(key, value) {
	const original = this[key];
	if (original instanceof Map) {
	  /* Serialize the Map as an object with a type and an array for the values */
	  return {
		dataType: "Map",
		value: [...original]
	  };
	} else {
	  return value;
	}
  }

  JSON_revive_maps(key, value) {
	if (typeof value === "object" && value !== null) {
	  if (value.dataType === "Map") {
		return new Map(value.value);
	  }
	}
	return value;
  }
  
  restore_state() {
	let bench_state = localStorage.getItem('bench_state');
	if (bench_state) {
	  let new_state = JSON.parse(bench_state, this.JSON_revive_maps);
	  new_state.state_is_live = true;
	  this.setState(new_state);
	}
  }

  save_state() {
	let bench_state = JSON.stringify(this.state, this.JSON_stringify_maps);
	localStorage.setItem('bench_state', bench_state);
  }

  add_relevant(docno) {
	let qrels = this.state.qrels;
	qrels.set(docno, true);
	this.setState({ qrels: qrels });
  }

  remove_relevant(docno) {
	let qrels = this.state.qrels;
	qrels.delete(docno);
	this.setState({ qrels: qrels });
  }

  change_writeup(name, value) {
	let writeup = this.state.writeup;
	writeup[name] = value;
	this.setState({ writeup: writeup });
  }

  save_topic() {
	let cur_topic = this.state.cur_topic;
	let topic_to_save = {
	  writeup: JSON.stringify(this.state.writeup),
	  qrels: JSON.stringify(this.state.qrels, this.JSON_stringify_maps)
	};
	let topics;
	if (cur_topic == -1) {
	  cur_topic = this.state.topics.length;
	  topics = [...this.state.topics, topic_to_save];
	} else {
	  topics = this.state.topics.splice(cur_topic, 1, topic_to_save);
	}
	this.setState({ topics: topics,
					cur_topic: cur_topic,
				  });
  }

  load_topic(topic_num) {
	if (topic_num < 0 || topic_num > this.state.topics.length)
	  return;
	let topic_to_load = this.state.topics[topic_num];
	this.setState({ cur_topic: topic_num,
					writeup: JSON.parse(topic_to_load.writeup, this.JSON_revive_maps),
					qrels: JSON.parse(topic_to_load.qrels, this.JSON_revive_maps)
				  });
  }

  delete_topic(topic_num) {
	if (topic_num < 0 || topic_num > this.state.topics.length)
	  return;
	let topics = this.state.topics.splice(topic_num, 1);
	this.setState({ cur_topic: -1,
					topics: topics
				  });
  }
  
  componentDidUpdate() {
	this.save_state();
  }
  
  render() {
	if (!this.state.hasOwnProperty('state_is_live')) {
	  this.restore_state();
	}
    return (
      <Tabs defaultActiveKey="search" id="workbench">
        <Tab eventKey="search" title="Search">
		  <SearchTab qrels={this.state.qrels}
					 add_relevant={this.add_relevant}
					 remove_relevant={this.remove_relevant}/>
        </Tab>
        <Tab eventKey="writeup" title="Write-Up">
          <Writeup qrels={this.state.qrels}
				   writeup={this.state.writeup}
				   change_writeup={this.change_writeup}
				   save_topic={this.save_topic}/>
        </Tab>
		<Tab eventKey="topics" title="My Topics">
		  <TopicList topics={this.state.topics}
					 load_topic={this.load_topic}
					 delete_topic={this.delete_topic}/>
		</Tab>
      </Tabs>
    );
  }
}

export { Workbench as default };
