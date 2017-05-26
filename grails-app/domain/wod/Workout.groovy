package wod

class Workout {

    String title
    static hasMany = [exercises: String]
    List exercises

    static constraints = {
    }
}
