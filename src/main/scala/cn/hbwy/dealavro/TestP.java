package cn.hbwy.dealavro;

public interface TestP {

    public void eee();

    public static class TestPim implements TestP{
        public static void main(String[] args) {
            new TestP.TestPim("");
        }
        public TestPim(String aaa){}
        @Override
        public void eee() {

        }
    }
}
