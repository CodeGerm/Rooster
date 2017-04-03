package org.cg.rooster.phoenix;

import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import org.cg.rooster.core.Condition;
import org.cg.rooster.core.Query;
import org.cg.rooster.core.QueryBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.domain.Sort.Order;

/**
 * Example usage of PhoenixJdbcDataRepository
 * @author WZ
 *
 */
public class TestPhoenixJdbcDataRepository {

	private EventDataRepository dataRepository;

	@Before
	public void setUp() throws Exception {
		dataRepository = new EventDataRepository();
	}

	@Test
	public void testSave() {
		Event Event = genEvent();
		dataRepository.save(Event);
		Assert.assertTrue(dataRepository.exists(Event.getId()));
	}

	@Test
	public void testSaveBatch() {
		List<Event> events = new LinkedList<Event>();
		for (int i = 0; i < 10; i++) {
			events.add(genEvent());
		}
		dataRepository.save(events);
		for ( Event e : events) {
			Assert.assertTrue(dataRepository.exists(e.getId()));
		}
	}

	@Test
	public void testExists() {
		Event e = genEvent();
		dataRepository.save(e);
		boolean isExist = dataRepository.exists(new Object[] {e.getTenantId(), e.getUserId(), e.getEventTime().getTime(), e.getReceiptTime().getTime()});
		Assert.assertTrue(isExist);
	}

	@Test
	public void testCount() {
		long count = dataRepository.count();
		System.out.println(count);
		//Assert.assertTrue(count == 100);
	}

	@Test
	public void testDeleteById() {
		Event event = genEvent();
		dataRepository.save(event);
		dataRepository.delete(event.getId());
		Assert.assertFalse(dataRepository.exists(event.getId()));
	}

	@Test
	public void testDeleteByIdBatch() {
		List<Event> events = new LinkedList<Event>();
		for (int i = 0; i < 10; i++) {
			events.add(genEvent());
		}
		dataRepository.save(events);
		List<Object[]> ids = new LinkedList<Object[]>();
		for (Event e : events) {
			Assert.assertTrue(dataRepository.exists(e.getId()));
			ids.add(e.getId());
		}
		dataRepository.delete(ids);
		for (Event e : events) {
			Assert.assertFalse(dataRepository.exists(e.getId()));
			ids.add(e.getId());
		}
	}

	@Test
	public void testFindOne() {
		Event e = genEvent();
		dataRepository.save(e);
		Event eo = dataRepository.get(new Object[] {e.getTenantId(), e.getUserId(), e.getEventTime().getTime(), e.getReceiptTime().getTime()});
		System.out.println(eo);
		Assert.assertNotNull(eo);
	}

	@Test
	public void testFindAll() {
		List<Event> list = (List<Event>) dataRepository.findAll();
		Assert.assertTrue(!list.isEmpty());
	}

	@Test
	public void testFindAllWithSort() {
		Query query = QueryBuilder.newBuilder()
				.sort(new Sort(new Order(Direction.DESC, "uid"), new Order(Direction.ASC, "event_time")))
				.build();
		List<Event> list = (List<Event>) dataRepository.find(query);
		Assert.assertTrue(!list.isEmpty());
	}

	@Test
	public void testFindAllWithLimit() {
		Query query = QueryBuilder.newBuilder()
				.limit(2)
				.build();
		List<Event> list = (List<Event>) dataRepository.find(query);
		Assert.assertTrue(!list.isEmpty());
		Assert.assertTrue(list.size()==2);
	}

	@Test
	public void testFindAllWithLimitAndSort() {
		Query query = QueryBuilder.newBuilder()
				.sort(new Sort(new Order(Direction.DESC, "uid"), new Order(Direction.ASC, "event_time")))
				.limit(2)
				.build();
		List<Event> list = (List<Event>) dataRepository.find(query);
		Assert.assertTrue(!list.isEmpty());
		Assert.assertTrue(list.size()==2);
	}

	@Test
	public void testFind() {
		List<Event> events = new LinkedList<Event>();
		Event event = genEvent();
		event.setEventTime(new Date(1436390151975l));
		events.add(event);
		Event event2 = genEvent();
		event2.setEventTime(new Date(1436216440707l));
		events.add(event2);
		dataRepository.save(events);		
		List<Condition> conditions = new LinkedList<Condition>();
		conditions.add(new Condition("uid", PhoenixConditionOperator.EQUAL,  event.getUserId()));
		Condition c1 = new Condition("event_time", PhoenixConditionOperator.IS_NOT_NULL, null);
		Condition c2 = new Condition("event_time", PhoenixConditionOperator.EQUAL, 1436216440707l);
		conditions.add(new Condition( c1, PhoenixConditionOperator.OR, c2 ) );
		
		Query query = QueryBuilder.newBuilder().conditions(conditions).build();
		
		List<Event> list = (List<Event>) dataRepository.find(query);
		Assert.assertTrue(!list.isEmpty());

		for (Event e : list) {
			System.out.println(e);
		}
	}

	@Test
	public void testFindWithSort() {
		List<Event> events = new LinkedList<Event>();
		Event event = genEvent();
		event.setEventTime(new Date(1436390151975l));
		events.add(event);
		Event event2 = genEvent();
		event2.setEventTime(new Date(1436216440707l));
		events.add(event2);
		dataRepository.save(events);
		List<Condition> conditions = new LinkedList<Condition>();
		conditions.add(new Condition("uid", PhoenixConditionOperator.EQUAL, event.getUserId()));
		Condition c1 = new Condition("event_time", PhoenixConditionOperator.EQUAL, 1436390151975l);
		Condition c2 = new Condition("event_time", PhoenixConditionOperator.EQUAL, 1436216440707l);
		conditions.add(new Condition( c1, PhoenixConditionOperator.OR, c2 ) );
		
		Query query = QueryBuilder.newBuilder()
				.conditions(conditions)
				.sort(new Sort(new Order(Direction.DESC, "receipt_time")))
				.build();

		List<Event> list = (List<Event>) dataRepository.find(query);
		Assert.assertTrue(!list.isEmpty());

		for (Event e : list) {
			System.out.println(e);
		}
	}

	@Test
	public void testFindWithLimit() {
		Event event = genEvent();
		event.setEventTime(new Date(1436390151975l));
		dataRepository.save(event);
		List<Condition> conditions = new LinkedList<Condition>();
		conditions.add(new Condition("uid", PhoenixConditionOperator.EQUAL, event.getUserId()));
		Condition c1 = new Condition("event_time", PhoenixConditionOperator.EQUAL, 1436390151975l);
		Condition c2 = new Condition("event_time", PhoenixConditionOperator.EQUAL, 1436216440707l);
		conditions.add(new Condition( c1, PhoenixConditionOperator.OR, c2 ) );
		
		Query query = QueryBuilder.newBuilder()
				.conditions(conditions)
				.limit(1)
				.build();
		
		List<Event> list = (List<Event>) dataRepository.find(query);
		Assert.assertTrue(!list.isEmpty());

		for (Event e : list) {
			System.out.println(e);
		}
	}

	@Test
	public void testFindWithLimitAndSort() {
		Event event = genEvent();
		event.setEventTime(new Date(1436390151975l));
		dataRepository.save(event);
		List<Condition> conditions = new LinkedList<Condition>();
		conditions.add(new Condition("uid", PhoenixConditionOperator.EQUAL, event.getUserId()));
		Condition c1 = new Condition("event_time", PhoenixConditionOperator.EQUAL, 1436390151975l);
		Condition c2 = new Condition("event_time", PhoenixConditionOperator.EQUAL, 1436216440707l);
		conditions.add(new Condition( c1, PhoenixConditionOperator.OR, c2 ) );
		
		Query query = QueryBuilder.newBuilder()
				.conditions(conditions)
				.sort(new Sort(new Order(Direction.DESC, "receipt_time")))
				.limit(1)
				.build();
		
		List<Event> list = (List<Event>) dataRepository.find(query);
		Assert.assertTrue(!list.isEmpty());

		for (Event e : list) {
			System.out.println(e);
		}
	}

	private static SecureRandom random = new SecureRandom();	
	
	private static Event genEvent() {
		Event event = new Event();
		event.setEventTime(new Date(1434441177000l));
		event.setMessage("TEST");
		event.setName("TEST_NAME");
		event.setReceiptTime(new Date(1434441175000l));
		event.setTenantId(1);
		event.setUserId(new BigInteger(130, random).toString(32));
		event.setVersion(1);
		return event;
	}

}
