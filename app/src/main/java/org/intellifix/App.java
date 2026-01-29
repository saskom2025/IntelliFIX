package org.intellifix;

public class App {
    public String getGreeting() {
        return "IntelliFIX started !!!";
    }

    public static void main(String[] args) {
        System.out.println(new App().getGreeting());
    }
}
