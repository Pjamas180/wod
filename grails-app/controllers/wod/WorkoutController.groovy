package wod

class WorkoutController {

    def index() { }

    def random() {
        def staticWorkout = new Workout(title: 'Helen', exercises: ['3 Rounds for Time', '400m Run',
                                                                    '21 Kettlebell Swings (1.5 pood)', '12 Pull Ups']
        )
        [ workout : staticWorkout ]
    }
}
