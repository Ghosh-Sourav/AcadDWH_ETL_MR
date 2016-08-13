package in.ac.iitkgp.acaddwh.background;

import java.sql.PreparedStatement;
import java.sql.SQLException;


public class Warehouser implements Runnable {

	private PreparedStatement ps = null;

	public Warehouser(PreparedStatement ps) {
		this.ps = ps;
	}

	@Override
	public void run() {
		try {
			ps.executeUpdate();
			//ps.close();
			//HiveConnection.closeConnection(con);
		} catch (SQLException e) {
			System.out.println("Warehouse thread failed due to: "+e.getMessage());
		}
	}

}
