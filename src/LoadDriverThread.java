
    public class LoadDriverThread extends Thread {
        private volatile boolean running = true;
        private int counter = 0;
        private String ip = null;

        /**
         * The constructor initialises the string for the IP-Address.
         * @param ipa A String of an IP-Address
         */
        public LoadDriverThread(String ipa) {
            ip = ipa;
        }
        /**
         * This method runs the thread and makes random transactions with the database. After a transaction the thread waits for 50 milliseconds to start the next one.
         */
        public void run() {
            System.out.println("Thread started.");
            Benchmark db = new Benchmark(ip);
            int random, delta, tellerid, branchid, accid;

            while(running) {
                random = (int) (Math.random() * 100 + 1);
                delta = (int) (Math.random() * 10000 + 1);
                accid = (int) (Math.random() * 10000000 + 1);
                tellerid = (int) (Math.random() * 1000 + 1);
                branchid = (int) (Math.random() * 100 + 1);

                if(random <= 50) {
                    //benchmark.einzahlungs_TX(accid, tellerid, branchid, delta, db.conn);
                    Benchmark.einzahlungs_TX(accid, tellerid, branchid, delta, db.conn);
                    counter++;
                }
                else if(random > 50 && random <= 85) {
                    //benchmark.kontostands_TX(accid, db.conn);
                    Benchmark.kontostands_TX(accid, db.conn);
                    counter++;
                }
                else {
                    //benchmark.analyse_TX(delta, db.conn);
                    Benchmark.analyse_TX(delta, db.conn);
                    counter++;
                }
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        /**
         * This method shuts the thread down by changing the boolean running to false
         */
        public void shutdown() {
            running = false;
        }
        /**
         * This method returns the count of transactions this thread has done
         * @return counter
         */
        public int getCounter() {
            return counter;
        }
        /**
         * This method sets the counter of transactions of this thread
         * @param n new counter
         */
        public void setCounter(int n) {
            this.counter = n;
        }

    }

