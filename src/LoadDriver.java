import java.io.IOException;
import java.util.Scanner;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class LoadDriver {
	static Logger logger = Logger.getLogger(LoadDriver.class.getName());
	static FileHandler fileHandler;

	/**
	 * This method asks the user for an IP-Address which is then scanned and returned
	 *
	 * @return scannedIP User-Input
	 */
	public static String scanIP() {
		Scanner scannerIP = new Scanner(System.in);
		System.out.println("Please enter a valid IP-Address (if you want to connect to the localhost, please confirm without input)\n");
		String scannedIP = scannerIP.nextLine();
		scannerIP.close();
		return scannedIP;
	}

	/**
	 * This method creates and starts five threads which do transactions with the database
	 *
	 * @param scannedIP the IP-Address which is used to connect to the Database
	 * @return t an array of Threads
	 */
	public static LoadDriverThread[] createThreads(String scannedIP) {
		LoadDriverThread[] t = new LoadDriverThread[5];
		t[0] = new LoadDriverThread(scannedIP);
		t[1] = new LoadDriverThread(scannedIP);
		t[2] = new LoadDriverThread(scannedIP);
		t[3] = new LoadDriverThread(scannedIP);
		t[4] = new LoadDriverThread(scannedIP);
		for (LoadDriverThread loadDriverThread : t) {
			loadDriverThread.start();
		}
		return t;

	}

	/**
	 * This method closes threads
	 *
	 * @param t an array of threads to be closed
	 */
	public static void deleteThreads(LoadDriverThread[] t) {
		for (int i = 0; i < t.length; i++) {
			t[i].shutdown();
		}
	}

	/**
	 * This method measures and prints the total transactions of the threads for a given time it also prints all transactions in a log file
	 *
	 * @param t           an array of threads
	 * @param decay_phase the time which is measured
	 * @return total_count the total count of transactions of all threads
	 */
	public static long measure(LoadDriverThread[] t, long decay_phase) {
		System.out.println("Starting measurement...");
		long total_count = 0;
		for (int i = 0; i < t.length; i++) {
			t[i].setCounter(0);
		}
		while (System.currentTimeMillis() < decay_phase) {
			total_count = t[0].getCounter() + t[1].getCounter()
					+ t[2].getCounter() + t[3].getCounter()
					+ t[4].getCounter();
			String output = t[0].getCounter() + ","
					+ t[1].getCounter() + ","
					+ t[2].getCounter() + ","
					+ t[3].getCounter() + ","
					+ t[4].getCounter() + ","
					+ "  Total TX: " + total_count;
			System.out.println(output);
			try {

				logger.setUseParentHandlers(false);
				fileHandler = new FileHandler("C:/benchmarkDatabase/CounterLog.log", true);
				logger.addHandler(fileHandler);
				SimpleFormatter formatter = new SimpleFormatter();
				fileHandler.setFormatter(formatter);
				logger.info(output);
				fileHandler.close();
			} catch (SecurityException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return total_count;
	}

	/**
	 * the main method
	 *
	 * @param args standard arguments
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		String scannedIP = scanIP();
		Benchmark.initDb(Benchmark.connect("127.0.0.1"), 100);
		LoadDriverThread[] threads = createThreads(scannedIP);
		long start = System.currentTimeMillis();
		long decay_phase = start + 240000;
		long time_phase = decay_phase + 300000;
		long end_phase = time_phase + 60000;
		System.out.println("Starting warmup phase...");
		while (System.currentTimeMillis() < decay_phase) {
			//Do not count, but transactions from LoadDriverThread will continue.
		}
		long timer1 = System.currentTimeMillis();
		System.out.println("Starting measure phase...");
		long total_count = measure(threads, time_phase);
		long timer2 = System.currentTimeMillis();
		long total_time = (timer2 - timer1) / 1000;
		long total_TX = total_count / total_time;
		System.out.println("Total TX: " + total_TX + " per second");

		System.out.println("Starting shutdown phase...");
		while (System.currentTimeMillis() < end_phase) {
			// Do not count, but transactions from LoadDriverThread will continue.
		}
		deleteThreads(threads);
		System.out.println("Program completed.");
	}
}
