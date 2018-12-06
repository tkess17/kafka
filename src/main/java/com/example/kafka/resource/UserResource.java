package com.example.kafka.resource;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.example.kafka.model.User;

@RestController
@RequestMapping("kafka")
public class UserResource {
	
	@Autowired
	private KafkaTemplate<String, User> kafkaTemplate;

	//@Value here to externalize topic
	private String testTopic = "testTopic";
	
	String fileName = "config/test.csv";
	
	@GetMapping("/publish/{name}")
	public String post(@PathVariable("name") final String name) throws IOException {
		
		ClassLoader classLoader = ClassLoader.getSystemClassLoader();
		File file = new File(classLoader.getResource(fileName).getFile());
		LineIterator it = FileUtils.lineIterator(file, "UTF-8");
		try {
			while(it.hasNext()) {
				String line = it.nextLine();
				String[] record = line.split(",");
				System.out.println(record[1] + record[2] + record[3] + record[4]);
				kafkaTemplate.send(testTopic, new User(record[1], record[2], record[3], record[4]));
			}
		} catch (Exception e) {
			System.out.println("oops : " + e);
		}
		return "Published Successfully!";
	}
}
