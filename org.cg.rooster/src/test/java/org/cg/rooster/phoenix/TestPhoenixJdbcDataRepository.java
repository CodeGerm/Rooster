package org.cg.rooster.phoenix;

import java.util.List;

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
	public void testExists() {
		boolean isExist = dataRepository.exists(new Object[] {1, "user1", 1434441175000l, 1434441177000l});
		Assert.assertTrue(isExist);
	}
	
	@Test
	public void testCount() {
		long count = dataRepository.count();
		System.out.println(count);
		//Assert.assertTrue(count == 100);
	}
	
	@Test
	public void testFindOne() {
		Event e = dataRepository.findOne(new Object[] {1, "user1", 1434441175000l, 1434441177000l});
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
				new Sort(
						new Order(Direction.DESC, "uid"), 
						new Order(Direction.ASC, "event_time")
						)
				);
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
				new Sort(
						new Order(Direction.DESC, "uid"), 
						new Order(Direction.ASC, "event_time")
						)
				, 2
				);
		Assert.assertTrue(!list.isEmpty());
		Assert.assertTrue(list.size()==2);
	}

}
