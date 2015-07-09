package org.cg.rooster.phoenix;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import org.cg.rooster.core.Condition;
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
		boolean isExist = dataRepository.exists(new Object[] {1, "TEST_USER", 1434441175000l, 1434441177000l});
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
		Event e = dataRepository.find(new Object[] {1, "TEST_USER", 1434441175000l, 1434441177000l});
		System.out.println(e);
		Assert.assertNull(e);
	}

	@Test
	public void testFindAll() {
		List<Event> list = (List<Event>) dataRepository.findAll();
		Assert.assertTrue(!list.isEmpty());
	}

	@Test
	public void testFindAllWithSort() {
		List<Event> list = (List<Event>) dataRepository.findAll(
				new Sort(new Order(Direction.DESC, "uid"), 
						new Order(Direction.ASC, "event_time")));
		Assert.assertTrue(!list.isEmpty());
	}

	@Test
	public void testFindAllWithLimit() {
		List<Event> list = (List<Event>) dataRepository.findAll(2);
		Assert.assertTrue(!list.isEmpty());
		Assert.assertTrue(list.size()==2);
	}

	@Test
	public void testFindAllWithLimitAndSort() {
		List<Event> list = (List<Event>) dataRepository.findAll(
				new Sort(new Order(Direction.DESC, "uid"), 
						new Order(Direction.ASC, "event_time"))
				, 2);
		Assert.assertTrue(!list.isEmpty());
		Assert.assertTrue(list.size()==2);
	}

	@Test
	public void testFind() {
		List<Condition> conditions = new LinkedList<Condition>();
		conditions.add(new Condition("uid", PhoenixConditionOperator.EQUAL, "TEST_USER"));
		Condition c1 = new Condition("event_time", PhoenixConditionOperator.IS_NOT_NULL, null);
		Condition c2 = new Condition("event_time", PhoenixConditionOperator.EQUAL, 1436216440707l);
		conditions.add(new Condition( c1, PhoenixConditionOperator.OR, c2 ) );
		List<Event> list = (List<Event>) dataRepository.find(conditions);
		Assert.assertTrue(!list.isEmpty());

		for (Event e : list) {
			System.out.println(e);
		}
	}

	@Test
	public void testFindWithSort() {
		List<Condition> conditions = new LinkedList<Condition>();
		conditions.add(new Condition("uid", PhoenixConditionOperator.EQUAL, "TEST_USER"));
		Condition c1 = new Condition("event_time", PhoenixConditionOperator.EQUAL, 1436390151975l);
		Condition c2 = new Condition("event_time", PhoenixConditionOperator.EQUAL, 1436216440707l);
		conditions.add(new Condition( c1, PhoenixConditionOperator.OR, c2 ) );
		List<Event> list = (List<Event>) dataRepository.find(conditions, new Sort(new Order(Direction.DESC, "receipt_time")));
		Assert.assertTrue(!list.isEmpty());

		for (Event e : list) {
			System.out.println(e);
		}
	}

	@Test
	public void testFindWithLimit() {
		List<Condition> conditions = new LinkedList<Condition>();
		conditions.add(new Condition("uid", PhoenixConditionOperator.EQUAL, "TEST_USER"));
		Condition c1 = new Condition("event_time", PhoenixConditionOperator.EQUAL, 1436390151975l);
		Condition c2 = new Condition("event_time", PhoenixConditionOperator.EQUAL, 1436216440707l);
		conditions.add(new Condition( c1, PhoenixConditionOperator.OR, c2 ) );
		List<Event> list = (List<Event>) dataRepository.find(conditions,1);
		Assert.assertTrue(!list.isEmpty());

		for (Event e : list) {
			System.out.println(e);
		}
	}

	@Test
	public void testFindWithLimitAndSort() {
		List<Condition> conditions = new LinkedList<Condition>();
		conditions.add(new Condition("uid", PhoenixConditionOperator.EQUAL, "TEST_USER"));
		Condition c1 = new Condition("event_time", PhoenixConditionOperator.EQUAL, 1436390151975l);
		Condition c2 = new Condition("event_time", PhoenixConditionOperator.EQUAL, 1436216440707l);
		conditions.add(new Condition( c1, PhoenixConditionOperator.OR, c2 ) );
		List<Event> list = (List<Event>) dataRepository.find(conditions, new Sort(new Order(Direction.DESC, "receipt_time")),1);
		Assert.assertTrue(!list.isEmpty());

		for (Event e : list) {
			System.out.println(e);
		}
	}

	private static Event genEvent() {
		Event event = new Event();
		event.setEventTime(new Date());
		event.setMessage("TEST");
		event.setName("TEST_NAME");
		event.setReceiptTime(new Date());
		event.setTenantId(1);
		event.setUserId("TEST_USER");
		event.setVersion(1);
		return event;
	}

}
