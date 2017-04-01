package org.cg.rooster.phoenix;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.LinkedHashMap;
import org.cg.rooster.core.RowColumnMapper;
import org.cg.rooster.core.TableDefinition;

/**
 * A Data Repository for {@link Event}
 * @author WZ
 *
 */
public class EventDataRepository extends PhoenixJdbcDataRepository<Event, Object[]> {
	
	public EventDataRepository() {
		super( dataSource, tableDef, ROW_COLUMN_MAPPER );
	}

	private final static PhoenixDataSource dataSource = new PhoenixDataSource("org.apache.phoenix.jdbc.PhoenixDriver", "jdbc:phoenix:10.0.1.31", true);
	
	private final static TableDefinition tableDef = new TableDefinition("Event", true, "tid", "uid", "event_time", "receipt_time");
	
	public static final RowColumnMapper<Event> ROW_COLUMN_MAPPER = new RowColumnMapper<Event>() {
		@Override
		public Event mapRow(ResultSet rs, int rowNum) throws SQLException {
			Event idEvent = new Event();
			idEvent.setTenantId(rs.getInt("tid"));
			idEvent.setUserId(rs.getString("uid"));
			idEvent.setEventTime(new Date(rs.getLong("event_time")));
			idEvent.setReceiptTime(new Date(rs.getLong("receipt_time")));
			idEvent.setName(rs.getString("name"));
			idEvent.setMessage(rs.getString("message"));
			idEvent.setVersion(rs.getInt("version"));
			return idEvent;
		}

		@Override
		public LinkedHashMap<String, Object> mapColumns(Event t) {
			LinkedHashMap<String, Object> columnMapping = new LinkedHashMap<String, Object>();
	        columnMapping.put("tid", t.getTenantId());
	        columnMapping.put("uid", t.getUserId());
	        columnMapping.put("event_time", t.getEventTime().getTime());
	        columnMapping.put("receipt_time", t.getReceiptTime().getTime());
	        columnMapping.put("name", t.getName());
	        columnMapping.put("message", t.getMessage());
	        columnMapping.put("version", t.getVersion());
	        return columnMapping;
		}
	};
}
