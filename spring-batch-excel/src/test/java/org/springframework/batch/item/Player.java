package org.springframework.batch.item;

import org.springframework.batch.item.excel.mapping.Column;

/**
 * Created by in329dei on 17-9-2014.
 */
public class Player {

    private String id;
    private String position;
    private String lastName;
    private String firstName;
    private double birthYear;
    private double debutYear;
    private String comment;

    @Column(name = "identificator")
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getPosition() {
        return position;
    }

    public void setPosition(String position) {
        this.position = position;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public double getBirthYear() {
        return birthYear;
    }

    public void setBirthYear(double birthYear) {
        this.birthYear = birthYear;
    }

    public double getDebutYear() {
        return debutYear;
    }
    
    @Column(name = "beginYear")
    public void setDebutYear(double debutYear) {
        this.debutYear = debutYear;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

	@Override
	public String toString() {
		return "Player [id=" + id + ", position=" + position + ", lastName="
				+ lastName + ", firstName=" + firstName + ", birthYear="
				+ birthYear + ", debutYear=" + debutYear + ", comment="
				+ comment + "]";
	}
}
